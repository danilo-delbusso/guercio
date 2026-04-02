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

var _ Reader = (*BlueskyReader)(nil)

type BlueskyReader struct {
	uri    string
	logger logger.Logger
}

func NewBlueskyReader(uri string, logger logger.Logger) *BlueskyReader {
	return &BlueskyReader{
		uri:    uri,
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

	// Jetstream payloads can occasionally exceed the default 32KB limit.
	// Set the read limit to 10MB to avoid "message too big" errors.
	conn.SetReadLimit(10 * 1024 * 1024)

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
			Type:       actType,
			AccountID:  v.Did,
			AccountURL: "https://bsky.app/profile/" + v.Did,
			PostID:     v.Commit.Record.Uri,
			Text:       v.Commit.Record.Text,
			CreatedAt:  v.Commit.Record.CreatedAt,
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
