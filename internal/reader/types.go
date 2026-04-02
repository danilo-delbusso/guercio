package reader

import (
	"context"

	"codeberg.org/dbus/guercio/internal/models"
)

type Reader interface {
	Run(ctx context.Context, out chan<- models.Activity) error
}
