package store

import (
	"context"

	"codeberg.org/dbus/botdetector/internal/models"
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
			        p.text = $text,
			        p.createdAt = datetime($createdAt)

			    MERGE (u)-[r:POSTED]->(p)
			    ON CREATE SET r.createdAt = datetime($createdAt)

			    WITH p
			    WHERE $parentUri IS NOT NULL AND $parentUri <> ""
			    MERGE (parent:Post {uri: $parentUri})
			    MERGE (p)-[:REPLIED_TO]->(parent)
			    RETURN p
			`
			params["uri"] = act.PostID
			params["text"] = act.Text
			params["createdAt"] = act.CreatedAt.Format("2006-01-02T15:04:05.999Z07:00")
			params["parentUri"] = act.ReplyToID

		case models.ActivityLike:
			if act.TargetID == "" {
				return nil, nil
			}
			query = `
			    MERGE (u:Account {did: $did})
			    ON CREATE SET u.url = $url
			    MERGE (p:Post {uri: $subjectUri})
			    MERGE (u)-[r:LIKES]->(p)
			    ON CREATE SET r.createdAt = datetime($createdAt)
			`
			params["subjectUri"] = act.TargetID
			params["createdAt"] = act.CreatedAt.Format("2006-01-02T15:04:05.999Z07:00")

		case models.ActivityRepost:
			if act.TargetID == "" {
				return nil, nil
			}
			query = `
			    MERGE (u:Account {did: $did})
			    ON CREATE SET u.url = $url
			    MERGE (p:Post {uri: $subjectUri})
			    MERGE (u)-[r:REPOSTED]->(p)
			    ON CREATE SET r.createdAt = datetime($createdAt)
			`
			params["subjectUri"] = act.TargetID
			params["createdAt"] = act.CreatedAt.Format("2006-01-02T15:04:05.999Z07:00")

		default:
			return nil, nil
		}

		return tx.Run(ctx, query, params)
	})

	return err
}

func (s *Neo4jStore) DetectHighSpeedBurst(ctx context.Context) (int64, error) {
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	query := `
		MATCH (a:Account)-[r]->()
		WHERE type(r) IN ['POSTED', 'LIKES', 'REPOSTED'] AND r.createdAt IS NOT NULL
		WITH a, r.createdAt AS t
		ORDER BY a, t
		WITH a, collect(t) AS times
		WHERE size(times) >= 4
		WITH a, [i IN range(0, size(times)-4) WHERE duration.between(times[i], times[i+3]).seconds < 15 | 1] AS bursts
		WHERE size(bursts) > 0
		SET a:Bot, a.reason = '4-action high-speed burst'
		RETURN count(a) as detected
	`

	res, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
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
	session := s.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	query := `
		MATCH (a1:Account)-[l12:LIKES]->(:Post)<-[:POSTED]-(a2:Account)
		WHERE id(a1) < id(a2)
		MATCH (a1)-[l13:LIKES]->(:Post)<-[:POSTED]-(a3:Account)
		WHERE id(a2) < id(a3)
		MATCH (a1)-[l14:LIKES]->(:Post)<-[:POSTED]-(a4:Account)
		WHERE id(a3) < id(a4)

		MATCH (a2)-[l21:LIKES]->(:Post)<-[:POSTED]-(a1),
		      (a2)-[l23:LIKES]->(:Post)<-[:POSTED]-(a3),
		      (a2)-[l24:LIKES]->(:Post)<-[:POSTED]-(a4),
		      (a3)-[l31:LIKES]->(:Post)<-[:POSTED]-(a1),
		      (a3)-[l32:LIKES]->(:Post)<-[:POSTED]-(a2),
		      (a3)-[l34:LIKES]->(:Post)<-[:POSTED]-(a4),
		      (a4)-[l41:LIKES]->(:Post)<-[:POSTED]-(a1),
		      (a4)-[l42:LIKES]->(:Post)<-[:POSTED]-(a2),
		      (a4)-[l43:LIKES]->(:Post)<-[:POSTED]-(a3)

		WITH a1, a2, a3, a4, [l12, l13, l14, l21, l23, l24, l31, l32, l34, l41, l42, l43] AS likes
		UNWIND likes AS l
		WITH a1, a2, a3, a4, min(l.createdAt) AS minT, max(l.createdAt) AS maxT
		WHERE duration.inSeconds(minT, maxT).seconds <= 600

		SET a1:Bot, a2:Bot, a3:Bot, a4:Bot,
		    a1.reason = 'Engagement Pod Detected',
		    a2.reason = 'Engagement Pod Detected',
		    a3.reason = 'Engagement Pod Detected',
		    a4.reason = 'Engagement Pod Detected'

		RETURN count(*) as pods
	`

	res, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
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
