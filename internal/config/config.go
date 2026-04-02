package config

import (
	"errors"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	Neo4jURI      string
	Neo4jUser     string
	Neo4jPassword string
	JetstreamURI  string
}

// Load reads configuration from the environment and validates required fields.
// It will attempt to load a .env file if present, but won't fail if it's missing,
// allowing system-level environment variables to take precedence.
func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{
		Neo4jURI:      os.Getenv("NEO4J_URI"),
		Neo4jUser:     os.Getenv("NEO4J_USER"),
		Neo4jPassword: os.Getenv("NEO4J_PASSWORD"),
		JetstreamURI:  os.Getenv("JETSTREAM_URI"),
	}

	if cfg.Neo4jURI == "" {
		return nil, errors.New("missing required environment variable: NEO4J_URI")
	}
	if cfg.Neo4jUser == "" {
		return nil, errors.New("missing required environment variable: NEO4J_USER")
	}
	if cfg.Neo4jPassword == "" {
		return nil, errors.New("missing required environment variable: NEO4J_PASSWORD")
	}
	if cfg.JetstreamURI == "" {
		return nil, errors.New("missing required environment variable: JETSTREAM_URI")
	}

	return cfg, nil
}
