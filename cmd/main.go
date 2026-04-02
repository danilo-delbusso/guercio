package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"codeberg.org/dbus/botdetector/internal/db"
	"codeberg.org/dbus/botdetector/internal/models"
	"codeberg.org/dbus/botdetector/internal/reader"
	"github.com/charmbracelet/log"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := log.NewWithOptions(os.Stderr, log.Options{
		TimeFormat: time.Kitchen,
	})

	dbUri := "bolt://localhost:7687"
	driver, err := neo4j.NewDriver(dbUri, neo4j.BasicAuth("neo4j", "s3cureP@ssword", ""))
	if err != nil {
		logger.Fatal("Failed to create Neo4j driver", "err", err)
	}
	defer driver.Close(ctx)

	logger.Info("Running Neo4j migrations...")
	if err := db.EnsureSchema(ctx, driver); err != nil {
		logger.Fatal("Migration failed", "err", err)
	}

	r := reader.New(logger)
	events := make(chan models.FirehoseEvent, 1000)

	go Neo4jWorker(ctx, events, driver, logger)
	go Run4SAnalysis(ctx, driver, logger)

	logger.Info("Starting Firehose Ingestion...")
	if err := r.Run(ctx, events); err != nil {
		logger.Error("Reader stopped", "err", err)
	}
}

func Neo4jWorker(ctx context.Context, events <-chan models.FirehoseEvent, driver neo4j.Driver, logger *log.Logger) {
	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}

			_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
				query := `
				    MERGE (u:Account {did: $did})
				    // Use MERGE here so if URI already exists, we just match it
				    // instead of trying to CREATE a duplicate and crashing
				    MERGE (p:Post {uri: $uri})
				    ON CREATE SET
				        p.text = $text,
				        p.createdAt = datetime()

				    // Use MERGE for the relationship to be safe
				    MERGE (u)-[:POSTED]->(p)

				    WITH p
				    WHERE $parentUri IS NOT NULL
				    MERGE (parent:Post {uri: $parentUri})
				    MERGE (p)-[:REPLIED_TO]->(parent)
				    RETURN p
				`

				var parentUri any = nil
				if event.Post.Reply != nil {
					parentUri = event.Post.Reply.Parent.Uri
				}

				params := map[string]any{
					"did":       event.User,
					"uri":       event.Post.Uri,
					"text":      event.Post.Text,
					"parentUri": parentUri,
				}
				return tx.Run(ctx, query, params)
			})

			if err != nil {
				logger.Error("Neo4j Insert Failed", "err", err)
			}
		}
	}
}

func Run4SAnalysis(ctx context.Context, driver neo4j.Driver, logger *log.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
			logger.Info("Running analysis")

			query := `
				MATCH (a:Account)-[:POSTED]->(p1:Post)-[:REPLIED_TO]->(p2:Post)-[:REPLIED_TO]->(p3:Post)-[:REPLIED_TO]->(p4:Post)
				WITH a, p1, p4
				WHERE duration.between(p4.createdAt, p1.createdAt).seconds < 15
				SET a:Bot, a.reason = '4-hop high-speed burst'
				RETURN count(a) as detected
			`

			res, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
				result, err := tx.Run(ctx, query, nil)
				if err != nil {
					return 0, err
				}
				if result.Next(ctx) {
					return result.Record().Values[0], nil
				}
				return 0, nil
			})

			if err != nil {
				logger.Error("Analysis failed", "err", err)
			} else {
				logger.Info("4S Analysis complete", "bots_found", res)
			}
			session.Close(ctx)
		}
	}
}
