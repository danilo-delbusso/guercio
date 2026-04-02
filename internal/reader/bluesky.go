package reader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"codeberg.org/dbus/botdetector/internal/logger"
	"codeberg.org/dbus/botdetector/internal/models"
	"github.com/coder/websocket"
)

// Reader defines the interface for any platform-specific event stream ingester.
type Reader interface {
	Run(ctx context.Context, out chan<- models.Activity) error
}

// Compile-time check to ensure BlueskyReader implements the Reader interface.
var _ Reader = (*BlueskyReader)(nil)

// BlueskyReader is the implementation of Reader for the Bluesky Jetstream firehose.
type BlueskyReader struct {
	uri    string
	logger logger.Logger
}

func NewBlueskyReader(logger logger.Logger) *BlueskyReader {
	return &BlueskyReader{
		uri:    "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.like&wantedCollections=app.bsky.feed.repost",
		logger: logger,
	}
}

func (r *BlueskyReader) Run(ctx context.Context, out chan<- models.Activity) error {
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
				Operation  string            `json:"operation"`
				Collection string            `json:"collection"`
				Uri        string            `json:"uri"`
				Record     models.PostRecord `json:"record"`
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

		var actType models.ActivityType
		switch v.Commit.Collection {
		case "app.bsky.feed.post":
			actType = models.ActivityPost
		case "app.bsky.feed.like":
			actType = models.ActivityLike
		case "app.bsky.feed.repost":
			actType = models.ActivityRepost
		default:
			continue
		}

		act := models.Activity{
			Type:      actType,
			AccountID: v.Did,
			PostID:    v.Commit.Record.Uri,
			Text:      v.Commit.Record.Text,
			CreatedAt: v.Commit.Record.CreatedAt,
		}

		if v.Commit.Record.Reply != nil {
			act.ReplyToID = v.Commit.Record.Reply.Parent.Uri
		}
		if v.Commit.Record.Subject != nil {
			act.TargetID = v.Commit.Record.Subject.Uri
		}

		select {
		case out <- act:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
