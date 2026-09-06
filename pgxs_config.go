package pgxs

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

type Shard struct {
	Name string `json:"name"`
	DSN  string `json:"dsn"`
}

type MappingEntry struct {
	Bucket BucketID `json:"bucket"`
	Shard  string   `json:"shard"`
}

type Config struct {
	Buckets      MaxBuckets     `json:"buckets"`
	SchemaPrefix string         `json:"schema_prefix"`
	Shards       []Shard        `json:"shards"`
	Mapping      []MappingEntry `json:"mapping"`
	Retry        RetryConfig
}

func LoadConfig(r io.Reader) (*Config, error) {
	var cfg Config
	if err := json.NewDecoder(r).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func LoadConfigFile(filename string) (*Config, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()
	return LoadConfig(f)
}

func (c *Config) Validate() error {
	if c.Buckets <= 0 {
		return fmt.Errorf("buckets must be positive, got %d", c.Buckets)
	}
	if strings.TrimSpace(c.SchemaPrefix) == "" {
		return fmt.Errorf("schema_prefix cannot be empty")
	}
	if len(c.Shards) == 0 {
		return fmt.Errorf("at least one shard required")
	}
	if len(c.Mapping) == 0 {
		return fmt.Errorf("mapping must be explicitly defined for all buckets")
	}

	shardNames := make(map[string]bool)
	for i, sh := range c.Shards {
		if strings.TrimSpace(sh.Name) == "" {
			return fmt.Errorf("shard[%d] name empty", i)
		}
		if shardNames[sh.Name] {
			return fmt.Errorf("duplicate shard name %q", sh.Name)
		}
		shardNames[sh.Name] = true
		if strings.TrimSpace(sh.DSN) == "" {
			return fmt.Errorf("shard %q DSN empty", sh.Name)
		}
	}

	usedBuckets := make(map[BucketID]bool)
	for _, entry := range c.Mapping {
		if c.Buckets.Less(entry.Bucket) {
			return fmt.Errorf("bucket %d out of range [0, %d)", entry.Bucket, c.Buckets)
		}
		if !shardNames[entry.Shard] {
			return fmt.Errorf("shard %q not found in shards list", entry.Shard)
		}
		if usedBuckets[entry.Bucket] {
			return fmt.Errorf("duplicate mapping for bucket %d", entry.Bucket)
		}
		usedBuckets[entry.Bucket] = true
	}
	// Проверяем, что все бакеты покрыты
	for bucket := range c.Buckets {
		if !usedBuckets[BucketID(bucket)] {
			return fmt.Errorf("bucket %d has no mapping", bucket)
		}
	}
	return nil
}
