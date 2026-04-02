package store

import (
	"context"

	"codeberg.org/dbus/guercio/internal/models"
)

// Store defines the data layer interface for bot detection
type Store interface {
	SaveActivity(ctx context.Context, act models.Activity) error
	DetectHighSpeedBurst(ctx context.Context) (int64, error)
	DetectEngagementPods(ctx context.Context) (int64, error)
	EnsureSchema(ctx context.Context) error
	Close(ctx context.Context) error
}
