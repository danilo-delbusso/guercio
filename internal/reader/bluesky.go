package reader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

	"codeberg.org/dbus/guercio/internal/logger"
	"codeberg.org/dbus/guercio/internal/models"
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
	var processed atomic.Int32
	var ignored atomic.Int32
	var failed atomic.Int32

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
		var v models.BlueskyJetstreamEvent

		if err := json.NewDecoder(frameReader).Decode(&v); err != nil {
			if err == io.EOF {
				ignored.Add(1)
				continue
			}
			failed.Add(1)
			r.logger.Error("Decode failed", "err", err)
			continue
		}

		// Only process 'commit' events
		if v.Kind != models.BlueskyEventKindCommit || v.Commit == nil {
			ignored.Add(1)
			continue
		}

		// Only process 'create' operations (ignore updates/deletes for now)
		if v.Commit.Operation != models.BlueskyEventOperationCreate {
			ignored.Add(1)
			continue
		}

		// Jetstream doesn't provide the URI directly in the commit, we must construct it
		uri := fmt.Sprintf("at://%s/%s/%s", v.Did, v.Commit.Collection, v.Commit.RKey)
		v.Commit.Record.Uri = uri

		var actType models.ActivityType
		switch v.Commit.Collection {
		case models.BlueskyCollectionPost:
			actType = models.ActivityPost
		case models.BlueskyCollectionLike:
			actType = models.ActivityLike
		case models.BlueskyCollectionRepost:
			actType = models.ActivityRepost
		default:
			ignored.Add(1)
			continue
		}

		act := models.Activity{
			Type:       actType,
			AccountID:  v.Did,
			AccountURL: "https://bsky.app/profile/" + v.Did,
			PostID:     v.Commit.Record.Uri,
			PostURL:    atUriToWebUrl(v.Commit.Record.Uri),
			Text:       v.Commit.Record.Text,
			CreatedAt:  v.Commit.Record.CreatedAt,
		}

		if v.Commit.Record.Reply != nil {
			act.ReplyToID = v.Commit.Record.Reply.Parent.Uri
			act.ReplyToURL = atUriToWebUrl(v.Commit.Record.Reply.Parent.Uri)
			act.ReplyToAuthorID = atUriToDid(v.Commit.Record.Reply.Parent.Uri)
		}
		if v.Commit.Record.Subject != nil {
			act.TargetID = v.Commit.Record.Subject.Uri
			act.TargetURL = atUriToWebUrl(v.Commit.Record.Subject.Uri)
			act.TargetAuthorID = atUriToDid(v.Commit.Record.Subject.Uri)
		}

		select {
		case out <- act:
			processed.Add(1)

			p := processed.Load()
			f := failed.Load()
			i := ignored.Load()

			if p%10_000 == 0 {
				r.logger.Info(fmt.Sprintf("Processed %d entries", p))
				r.logger.Info(fmt.Sprintf("Failed %d entries", f))
				r.logger.Info(fmt.Sprintf("Ignored %d entries", i))
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func atUriToDid(uri string) string {
	parts := strings.Split(uri, "/")
	if len(parts) >= 3 && parts[0] == "at:" {
		return parts[2]
	}
	return ""
}

func atUriToWebUrl(uri string) string {
	parts := strings.Split(uri, "/")
	if len(parts) >= 5 && parts[0] == "at:" && parts[3] == "app.bsky.feed.post" {
		return "https://bsky.app/profile/" + parts[2] + "/post/" + parts[4]
	}
	return ""
}
