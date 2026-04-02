package detection

import (
	"context"
	"time"

	"codeberg.org/dbus/botdetector/internal/models"
	"codeberg.org/dbus/botdetector/internal/store"
	"github.com/charmbracelet/log"
)

type Service struct {
	store  store.Store
	logger *log.Logger
}

func NewService(store store.Store, logger *log.Logger) *Service {
	return &Service{
		store:  store,
		logger: logger,
	}
}

func (s *Service) Ingest(ctx context.Context, events <-chan models.Activity) {
	for {
		select {
		case <-ctx.Done():
			return
		case act, ok := <-events:
			if !ok {
				return
			}
			if err := s.store.SaveActivity(ctx, act); err != nil {
				s.logger.Error("Failed to save activity", "err", err)
			}
		}
	}
}

func (s *Service) RunAnalysisLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.logger.Info("Running Bot Detection Analysis...")

			count, err := s.store.DetectHighSpeedBurst(ctx)
			if err != nil {
				s.logger.Error("4-hop Analysis failed", "err", err)
			} else {
				s.logger.Info("4-hop Analysis complete", "bots_found", count)
			}

			pods, err := s.store.DetectEngagementPods(ctx)
			if err != nil {
				s.logger.Error("Engagement Pod Analysis failed", "err", err)
			} else {
				s.logger.Info("Engagement Pod Analysis complete", "pods_found", pods)
			}
		}
	}
}
