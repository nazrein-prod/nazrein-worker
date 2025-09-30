package db

import (
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

func ConnectRedis() (*redis.Client, error) {

	url := os.Getenv("UPSTASH_REDIS_URL")
	if url == "" {
		return nil, fmt.Errorf("UPSTASH_REDIS_URL is not set")
	}

	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis url: %w", err)
	}

	client := redis.NewClient(opt)
	fmt.Println("Connected to Redis...")
	return client, nil
}
