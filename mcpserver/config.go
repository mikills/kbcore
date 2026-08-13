package mcpserver

import (
	"time"

	"github.com/mikills/minnow/kb"
)

type CodeIndexDefaults struct {
	Include          []string
	Exclude          []string
	MaxFileBytes     int64
	ChunkSize        int
	ChunkOverlap     int
	IncludeUntracked bool
	ResourcePolicy   kb.CodeIndexResourcePolicy
	RequireConfirm   bool
}

type Config struct {
	Enabled            bool
	HTTPEnabled        bool
	StdioEnabled       bool
	HTTPPath           string
	ReadOnly           bool
	AllowIndexing      bool
	AllowSyncIndexing  bool
	AllowDestructive   bool
	AllowAdmin         bool
	DefaultSyncTimeout time.Duration
	MaxSyncTimeout     time.Duration
	HTTPJSONResponse   bool
	HTTPStateless      bool
	// HTTPStateful explicitly opts into retained streamable-HTTP sessions.
	// HTTPStateless defaults to true when this is false.
	HTTPStateful       bool
	HTTPSessionTimeout time.Duration
	HTTPMaxSessions    int
	CodeIndex          CodeIndexDefaults
}

func (c Config) normalized() Config {
	if c.HTTPPath == "" {
		c.HTTPPath = "/mcp"
	}
	if c.DefaultSyncTimeout <= 0 {
		c.DefaultSyncTimeout = 30 * time.Second
	}
	if c.MaxSyncTimeout <= 0 {
		c.MaxSyncTimeout = 2 * time.Minute
	}
	if c.DefaultSyncTimeout > c.MaxSyncTimeout {
		c.DefaultSyncTimeout = c.MaxSyncTimeout
	}
	if c.HTTPStateful {
		c.HTTPStateless = false
	} else {
		c.HTTPStateless = true
	}
	if c.HTTPSessionTimeout <= 0 {
		c.HTTPSessionTimeout = 30 * time.Minute
	}
	if c.HTTPMaxSessions <= 0 {
		c.HTTPMaxSessions = 128
	}
	return c
}
