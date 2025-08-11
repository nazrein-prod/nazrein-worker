package config

import (
	"context"
	"time"
)

type Config struct {
	Ctx                  context.Context
	MaxRetries           int
	RetryKeyPrefix       string
	DeadLetterStreamName string
	ImageEtagPrefix      string
	ImageEtagTTL         time.Duration
	StreamName           string
	GroupName            string
	ConsumerID           string
	BatchSize            int
	BlockTime            time.Duration
}

func NewConfig() *Config {
	return &Config{
		Ctx:                  context.Background(),
		MaxRetries:           3,
		RetryKeyPrefix:       "retry:msgid:",
		DeadLetterStreamName: "nazrein:dead",
		ImageEtagPrefix:      "img_etag:",
		ImageEtagTTL:         24 * time.Hour,
		StreamName:           "nazrein",
		GroupName:            "group1",
		ConsumerID:           "consumer1",
		BatchSize:            50,
		BlockTime:            30 * time.Second,
	}
}
