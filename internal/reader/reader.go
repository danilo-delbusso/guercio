package reader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"codeberg.org/dbus/botdetector/internal/models"
	"github.com/charmbracelet/log"
	"github.com/coder/websocket"
)

type Reader struct {
	uri    string
	logger *log.Logger
}

func New(logger *log.Logger) Reader {
	return Reader{
		uri:    "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.like&wantedCollections=app.bsky.feed.repost",
		logger: logger,
	}
}

func (r Reader) Run(ctx context.Context, out chan<- models.FirehoseEvent) error {
	r.logger.Info("Connecting to Jetstream...", "uri", r.uri)

	conn, _, err := websocket.Dial(ctx, r.uri, nil)
	if err != nil {
		return fmt.Errorf("dial error: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "stopping")

	r.logger.Info("Connected! Streaming to channel...")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, frameReader, err := conn.Reader(ctx)
		if err != nil {
			return err
		}

		// Jetstream wraps the record inside 'commit'
		var v struct {
			Did    string `json:"did"`
			Commit struct {
				Operation string            `json:"operation"`
				Uri       string            `json:"uri"`
				Record    models.PostRecord `json:"record"`
			} `json:"commit"`
		}

		if err := json.NewDecoder(frameReader).Decode(&v); err != nil {
			if err == io.EOF {
				continue
			}
			r.logger.Error("Decode failed", "err", err)
			continue
		}
		v.Commit.Record.Uri = v.Commit.Uri

		select {
		case out <- models.FirehoseEvent{User: v.Did, Post: v.Commit.Record}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
