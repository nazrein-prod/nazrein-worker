package config

import (
	"context"
	"log"
	"os"
	"strconv"
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
	ImagekitFolder       string
}

func getEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("Env missing: %s", key)
	}

	return val
}

func NewConfig() *Config {
	maxRetriesStr := getEnv("MAX_RETRIES")
	imageEtagTTLStr := getEnv("IMAGE_ETAG_TTL")
	batchSizeStr := getEnv("REDIS_BATCH_SIZE")
	blockTimeStr := getEnv("REDIS_STREAM_BLOCK_TIME")

	maxRetries, err := strconv.Atoi(maxRetriesStr)
	if err != nil {
		log.Printf("invalid MAX_RETRIES=%s, defaulting to 3", maxRetriesStr)
		maxRetries = 3
	}

	batchSize, err := strconv.Atoi(batchSizeStr)
	if err != nil {
		log.Printf("invalid REDIS_BATCH_SIZE=%s, defaulting to 50", batchSizeStr)
		batchSize = 50
	}

	imageEtagTTL, err := time.ParseDuration(imageEtagTTLStr)
	if err != nil {
		log.Printf("invalid IMAGE_ETAG_TTL=%s, defaulting to 24h", imageEtagTTLStr)
		imageEtagTTL = 24 * time.Hour
	}

	blockTime, err := time.ParseDuration(blockTimeStr)
	if err != nil {
		log.Printf("invalid REDIS_STREAM_BLOCK_TIME=%s, defaulting to 1m", blockTimeStr)
		blockTime = 1 * time.Minute
	}

	return &Config{
		Ctx:                  context.Background(),
		MaxRetries:           maxRetries,
		RetryKeyPrefix:       getEnv("REDIS_RETRY_KEY_PREFIX"),
		DeadLetterStreamName: getEnv("DEAD_LETTER_STREAM_NAME"),
		ImageEtagPrefix:      getEnv("IMAGE_ETAG_PREFIX"),
		ImageEtagTTL:         imageEtagTTL,
		StreamName:           getEnv("REDIS_STREAM_NAME"),
		GroupName:            getEnv("REDIS_GROUP_NAME"),
		ConsumerID:           getEnv("REDIS_CONSUMER_ID"),
		BatchSize:            batchSize,
		BlockTime:            blockTime,
		ImagekitFolder:       getEnv("IMAGEKIT_FOLDER"),
	}
}
