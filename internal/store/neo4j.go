package store

import (
	"context"

	"codeberg.org/dbus/guercio/internal/models"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
)

type Neo4jStore struct {
	driver neo4j.Driver
}

func NewNeo4jStore(driver neo4j.Driver) *Neo4jStore {
	return &Neo4jStore{driver: driver}
}

func (s *Neo4jStore) EnsureSchema(ctx context.Context) error {
	schemas := []string{
		`CREATE CONSTRAINT account_did IF NOT EXISTS FOR (a:Account) REQUIRE a.did IS UNIQUE`,
		`CREATE CONSTRAINT post_uri IF NOT EXISTS FOR (p:Post) REQUIRE p.uri IS UNIQUE`,
		`CREATE INDEX post_created_at IF NOT EXISTS FOR (p:Post) ON (p.createdAt)`,
		`CREATE INDEX likes_created_at IF NOT EXISTS FOR ()-[r:LIKES]-() ON (r.createdAt)`,
		`CREATE INDEX posted_created_at IF NOT EXISTS FOR ()-[r:POSTED]-() ON (r.createdAt)`,
		`CREATE INDEX reposted_created_at IF NOT EXISTS FOR ()-[r:REPOSTED]-() ON (r.createdAt)`,
		`CREATE FULLTEXT INDEX post_text_search IF NOT EXISTS FOR (p:Post) ON EACH [p.text]`,
	}

	session := s.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
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

func (s *Neo4jStore) Close(ctx context.Context) error {
	return s.driver.Close(ctx)
}

func (s *Neo4jStore) SaveActivity(ctx context.Context, act models.Activity) error {
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		var query string
		params := map[string]any{
			"did": act.AccountID,
			"url": act.AccountURL,
		}

		switch act.Type {
		case models.ActivityPost:
			query = `
			    MERGE (u:Account {did: $did})
			    ON CREATE SET u.url = $url
			    MERGE (p:Post {uri: $uri})
			    ON CREATE SET
			        p.url = $postUrl,
			        p.text = $text,
			        p.createdAt = datetime($createdAt)

			    MERGE (u)-[r:POSTED]->(p)
			    ON CREATE SET r.createdAt = datetime($createdAt)

			    WITH p
			    WHERE $parentUri IS NOT NULL AND $parentUri <> ""
			    MERGE (parent:Post {uri: $parentUri})
			    ON CREATE SET parent.url = $parentUrl
			    MERGE (p)-[:REPLIED_TO]->(parent)

                FOREACH (ignore IN CASE WHEN $parentAuthorDid IS NOT NULL AND $parentAuthorDid <> "" THEN [1] ELSE [] END |
                    MERGE (parentAuthor:Account {did: $parentAuthorDid})
                    ON CREATE SET parentAuthor.url = $parentAuthorUrl
                    MERGE (parentAuthor)-[:POSTED]->(parent)
                )

			    RETURN p
			`
			params["uri"] = act.PostID
			params["postUrl"] = act.PostURL
			params["text"] = act.Text
			params["createdAt"] = act.CreatedAt.Format("2006-01-02T15:04:05.999Z07:00")
			params["parentUri"] = act.ReplyToID
			params["parentUrl"] = act.ReplyToURL
			params["parentAuthorDid"] = act.ReplyToAuthorID
			parentAuthorUrl := ""
			if act.ReplyToAuthorID != "" {
				parentAuthorUrl = "https://bsky.app/profile/" + act.ReplyToAuthorID
			}
			params["parentAuthorUrl"] = parentAuthorUrl

		case models.ActivityLike:
			if act.TargetID == "" {
				return nil, nil
			}
			query = `
			    MERGE (u:Account {did: $did})
			    ON CREATE SET u.url = $url
			    MERGE (p:Post {uri: $subjectUri})
			    ON CREATE SET p.url = $targetUrl
			    MERGE (u)-[r:LIKES]->(p)
			    ON CREATE SET r.createdAt = datetime($createdAt)

                FOREACH (ignore IN CASE WHEN $targetAuthorDid IS NOT NULL AND $targetAuthorDid <> "" THEN [1] ELSE [] END |
                    MERGE (targetAuthor:Account {did: $targetAuthorDid})
                    ON CREATE SET targetAuthor.url = $targetAuthorUrl
                    MERGE (targetAuthor)-[:POSTED]->(p)
                )
			`
			params["subjectUri"] = act.TargetID
			params["targetUrl"] = act.TargetURL
			params["createdAt"] = act.CreatedAt.Format("2006-01-02T15:04:05.999Z07:00")
			params["targetAuthorDid"] = act.TargetAuthorID
			targetAuthorUrl := ""
			if act.TargetAuthorID != "" {
				targetAuthorUrl = "https://bsky.app/profile/" + act.TargetAuthorID
			}
			params["targetAuthorUrl"] = targetAuthorUrl

		case models.ActivityRepost:
			if act.TargetID == "" {
				return nil, nil
			}
			query = `
			    MERGE (u:Account {did: $did})
			    ON CREATE SET u.url = $url
			    MERGE (p:Post {uri: $subjectUri})
			    ON CREATE SET p.url = $targetUrl
			    MERGE (u)-[r:REPOSTED]->(p)
			    ON CREATE SET r.createdAt = datetime($createdAt)

                FOREACH (ignore IN CASE WHEN $targetAuthorDid IS NOT NULL AND $targetAuthorDid <> "" THEN [1] ELSE [] END |
                    MERGE (targetAuthor:Account {did: $targetAuthorDid})
                    ON CREATE SET targetAuthor.url = $targetAuthorUrl
                    MERGE (targetAuthor)-[:POSTED]->(p)
                )
			`
			params["subjectUri"] = act.TargetID
			params["targetUrl"] = act.TargetURL
			params["createdAt"] = act.CreatedAt.Format("2006-01-02T15:04:05.999Z07:00")
			params["targetAuthorDid"] = act.TargetAuthorID
			targetAuthorUrl := ""
			if act.TargetAuthorID != "" {
				targetAuthorUrl = "https://bsky.app/profile/" + act.TargetAuthorID
			}
			params["targetAuthorUrl"] = targetAuthorUrl

		default:
			return nil, nil
		}

		return tx.Run(ctx, query, params)
	})

	return err
}

func (s *Neo4jStore) DetectHighSpeedBurst(ctx context.Context) (int64, error) {
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	query := `
		MATCH (a:Account)-[r]->()
		WHERE type(r) IN ['POSTED', 'LIKES', 'REPOSTED'] AND r.createdAt IS NOT NULL
		WITH a, r.createdAt AS t
		ORDER BY a, t
		WITH a, collect(t) AS times
		WHERE size(times) >= 4
		WITH a, [i IN range(0, size(times)-4) WHERE duration.between(times[i], times[i+3]).seconds < 1 | 1] AS bursts
		WHERE size(bursts) > 0
		RETURN count(a) as detected
	`

	res, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, query, nil)
		if err != nil {
			return int64(0), err
		}
		if result.Next(ctx) {
			val := result.Record().Values[0]
			if v, ok := val.(int64); ok {
				return v, nil
			}
		}
		return int64(0), nil
	})

	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}

func (s *Neo4jStore) DetectEngagementPods(ctx context.Context) (int64, error) {
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	query := `
		// Stepwise clique building to prevent combinatorial explosion and OOM

		// 1. Find mutual pairs
		MATCH (a1:Account)-[l12:LIKES]->(:Post)<-[:POSTED]-(a2:Account),
		      (a2)-[l21:LIKES]->(:Post)<-[:POSTED]-(a1)
		WHERE elementId(a1) < elementId(a2)
		WITH a1, a2, l12, l21

		// 2. Find mutual triads
		MATCH (a1)-[l13:LIKES]->(:Post)<-[:POSTED]-(a3:Account),
		      (a3)-[l31:LIKES]->(:Post)<-[:POSTED]-(a1),
		      (a2)-[l23:LIKES]->(:Post)<-[:POSTED]-(a3),
		      (a3)-[l32:LIKES]->(:Post)<-[:POSTED]-(a2)
		WHERE elementId(a2) < elementId(a3)
		WITH a1, a2, a3, l12, l21, l13, l31, l23, l32

		// 3. Find mutual tetrads (clique size 4)
		MATCH (a1)-[l14:LIKES]->(:Post)<-[:POSTED]-(a4:Account),
		      (a4)-[l41:LIKES]->(:Post)<-[:POSTED]-(a1),
		      (a2)-[l24:LIKES]->(:Post)<-[:POSTED]-(a4),
		      (a4)-[l42:LIKES]->(:Post)<-[:POSTED]-(a2),
		      (a3)-[l34:LIKES]->(:Post)<-[:POSTED]-(a4),
		      (a4)-[l43:LIKES]->(:Post)<-[:POSTED]-(a3)
		WHERE elementId(a3) < elementId(a4)

		// 4. Check temporal constraint on the whole clique's activity
		WITH [l12.createdAt, l21.createdAt, l13.createdAt, l31.createdAt, l23.createdAt, l32.createdAt, l14.createdAt, l41.createdAt, l24.createdAt, l42.createdAt, l34.createdAt, l43.createdAt] AS times
		UNWIND times AS t
		WITH min(t) AS minT, max(t) AS maxT
		WHERE duration.between(minT, maxT).seconds <= 600

		RETURN count(*) as pods
	`

	res, err := session.ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, query, nil)
		if err != nil {
			return int64(0), err
		}
		if result.Next(ctx) {
			val := result.Record().Values[0]
			if v, ok := val.(int64); ok {
				return v, nil
			}
		}
		return int64(0), nil
	})

	if err != nil {
		return 0, err
	}
	return res.(int64), nil
}
