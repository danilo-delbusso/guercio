package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"codeberg.org/dbus/guercio/internal/config"
	"codeberg.org/dbus/guercio/internal/detection"
	"codeberg.org/dbus/guercio/internal/models"
	"codeberg.org/dbus/guercio/internal/reader"
	"codeberg.org/dbus/guercio/internal/store"
	"github.com/charmbracelet/log"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := log.NewWithOptions(os.Stderr, log.Options{
		TimeFormat: time.Kitchen,
	})

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Configuration error", "err", err)
	}

	driver, err := neo4j.NewDriver(cfg.Neo4jURI, neo4j.BasicAuth(cfg.Neo4jUser, cfg.Neo4jPassword, ""))
	if err != nil {
		logger.Fatal("Failed to create Neo4j driver", "err", err)
	}
	defer driver.Close(ctx)

	neoStore := store.NewNeo4jStore(driver)

	logger.Info("Running Neo4j migrations...")
	if err := neoStore.EnsureSchema(ctx); err != nil {
		logger.Fatal("Migration failed", "err", err)
	}

	var r reader.Reader = reader.NewBlueskyReader(cfg.JetstreamURI, logger)
	events := make(chan models.Activity, 1000)

	detService := detection.NewService(neoStore, logger)

	go detService.Ingest(ctx, events)
	go detService.RunAnalysisLoop(ctx, 5*time.Second)

	logger.Info("Starting Firehose Ingestion...")
	if err := r.Run(ctx, events); err != nil {
		logger.Error("Reader stopped", "err", err)
	}
}
