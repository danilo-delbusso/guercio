package db

import (
	"context"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

func EnsureSchema(ctx context.Context, driver neo4j.Driver) error {
	schemas := []string{
		// Constraints (Unique IDs)
		`CREATE CONSTRAINT account_did IF NOT EXISTS FOR (a:Account) REQUIRE a.did IS UNIQUE`,
		`CREATE CONSTRAINT post_uri IF NOT EXISTS FOR (p:Post) REQUIRE p.uri IS UNIQUE`,

		// Range Indexes (For fast 4-hop traversals)
		`CREATE INDEX post_created_at IF NOT EXISTS FOR (p:Post) ON (p.createdAt)`,

		// Vector/Fulltext (If you want to find "Bot-like" text patterns)
		`CREATE FULLTEXT INDEX post_text_search IF NOT EXISTS FOR (p:Post) ON EACH [p.text]`,
	}

	session := driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	for _, query := range schemas {
		_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
			return tx.Run(ctx, query, nil)
		})
		if err != nil {
			return err
		}
	}
	return nil
}
