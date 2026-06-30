package updater

import (
	"context"
	"fmt"
	"github.com/Layr-Labs/eigenlayer-rewards-proofs/pkg/utils"
	"github.com/Layr-Labs/eigenlayer-rewards-updater/internal/metrics"
	"github.com/Layr-Labs/eigenlayer-rewards-updater/pkg/services"
	"github.com/Layr-Labs/eigenlayer-rewards-updater/pkg/sidecar"
	rewardsV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/rewards"
	"go.uber.org/zap"
	ddTracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"time"
)

type Updater struct {
	transactor    services.Transactor
	logger        *zap.Logger
	sidecarClient *sidecar.SidecarClient
	// waitForGeneration controls whether GenerateRewards blocks until the sidecar
	// finishes computing rewards for the cutoff (WaitForComplete). When false, the
	// updater relies on an external generator (e.g. the sidecar-rewards-refresher
	// cron) and skips the run if the root isn't ready yet, instead of blocking for
	// the full multi-hour mainnet computation.
	waitForGeneration bool
}

func NewUpdater(
	transactor services.Transactor,
	sc *sidecar.SidecarClient,
	logger *zap.Logger,
	waitForGeneration bool,
) (*Updater, error) {
	return &Updater{
		transactor:        transactor,
		logger:            logger,
		sidecarClient:     sc,
		waitForGeneration: waitForGeneration,
	}, nil
}

type UpdatedRoot struct {
	SnapshotDate string
	Root         string
}

// Update fetches the most recent snapshot and the most recent submitted timestamp from the chain.
func (u *Updater) Update(ctx context.Context) (*UpdatedRoot, error) {
	span, ctx := ddTracer.StartSpanFromContext(ctx, "updater::Update")
	defer span.Finish()

	u.logger.Sugar().Infow("Resolving latest rewards snapshot",
		zap.Bool("wait_for_generation", u.waitForGeneration),
	)
	// WaitForComplete=false resolves the latest cutoff and ensures generation is
	// enqueued, but returns immediately rather than blocking for the (multi-hour
	// on mainnet) computation. WaitForComplete=true preserves the prior behavior.
	res, err := u.sidecarClient.Rewards.GenerateRewards(ctx, &rewardsV1.GenerateRewardsRequest{
		RespondWithRewardsData: false,
		WaitForComplete:        u.waitForGeneration,
		CutoffDate:             "latest",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate rewards: %w", err)
	}

	u.logger.Sugar().Infow("Generating a rewards root",
		zap.String("cutoffDate", res.CutoffDate),
	)
	rootRes, err := u.sidecarClient.Rewards.GenerateRewardsRoot(ctx, &rewardsV1.GenerateRewardsRootRequest{
		CutoffDate: res.CutoffDate,
	})
	if err != nil {
		// When we don't block on generation, the root may simply not be computed
		// yet (the refresher is still running). Treat that as a healthy skip rather
		// than a failure, so the daily cron doesn't alarm; the next run (after the
		// refresher completes) will post it.
		if !u.waitForGeneration {
			u.logger.Sugar().Warnw("Rewards root not available yet; skipping this run (generation may still be in progress)",
				zap.String("cutoffDate", res.CutoffDate),
				zap.Error(err),
			)
			metrics.GetStatsdClient().Incr(metrics.Counter_UpdateNoUpdate, nil, 1)
			metrics.IncCounterUpdateRun(metrics.CounterUpdateRunsNoUpdate)
			return nil, nil
		}
		return nil, fmt.Errorf("failed to generate rewards root: %w", err)
	}

	u.logger.Sugar().Debugw("Rewards snapshot generated",
		zap.String("rewardsCalculationEndDate", rootRes.RewardsCalcEndDate),
		zap.String("root", rootRes.RewardsRoot),
	)

	rewardsCalcEnd, err := time.Parse(time.DateOnly, rootRes.RewardsCalcEndDate)
	if err != nil {
		return nil, fmt.Errorf("failed to parse snapshot date: %w", err)
	}

	rootBytes, err := utils.ConvertStringToBytes(rootRes.RewardsRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to convert root to bytes: %w", err)
	}

	// send the merkle root to the smart contract
	u.logger.Sugar().Infow("updating rewards", zap.String("new_root", rootRes.RewardsRoot))

	u.logger.Sugar().Infow("Calculated timestamp",
		zap.Int64("calculated_until_timestamp", rewardsCalcEnd.Unix()),
		zap.String("calculated_until_date", rewardsCalcEnd.Format(time.DateOnly)),
	)

	// Skip submission when the computed root is not newer than what is already
	// on-chain. The contract requires a strictly newer rewardsCalculationEndTimestamp,
	// so submitting an equal/older one would revert. Treating this as a successful
	// no-op (rather than a failed submit) keeps the daily cron's exit status and
	// metrics honest: real failures stay distinguishable from "nothing to do".
	newCalcEnd := uint32(rewardsCalcEnd.Unix())
	currentCalcEnd, err := u.transactor.CurrRewardsCalculationEndTimestamp()
	if err != nil {
		return nil, fmt.Errorf("failed to get current rewards calculation end timestamp: %w", err)
	}
	if newCalcEnd <= currentCalcEnd {
		u.logger.Sugar().Infow("No new rewards root to submit; on-chain root is already current",
			zap.Uint32("on_chain_calc_end", currentCalcEnd),
			zap.Uint32("computed_calc_end", newCalcEnd),
			zap.String("root", rootRes.RewardsRoot),
		)
		metrics.GetStatsdClient().Incr(metrics.Counter_UpdateNoUpdate, nil, 1)
		metrics.IncCounterUpdateRun(metrics.CounterUpdateRunsNoUpdate)
		return &UpdatedRoot{
			SnapshotDate: rootRes.RewardsCalcEndDate,
			Root:         rootRes.RewardsRoot,
		}, nil
	}

	if err := u.transactor.SubmitRoot(ctx, [32]byte(rootBytes), newCalcEnd); err != nil {
		metrics.GetStatsdClient().Incr(metrics.Counter_UpdateFails, nil, 1)
		metrics.IncCounterUpdateRun(metrics.CounterUpdateRunsFailed)
		u.logger.Sugar().Errorw("Failed to submit root", zap.Error(err))
		return nil, err
	} else {
		metrics.GetStatsdClient().Incr(metrics.Counter_UpdateSuccess, nil, 1)
		metrics.IncCounterUpdateRun(metrics.CounterUpdateRunsSuccess)
	}

	return &UpdatedRoot{
		SnapshotDate: rootRes.RewardsCalcEndDate,
		Root:         rootRes.RewardsRoot,
	}, nil
}
