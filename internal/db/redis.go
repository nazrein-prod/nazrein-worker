package db

import (
	"fmt"

	"github.com/redis/go-redis/v9"
)

func ConnectRedis() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	fmt.Println("Connected to Redis...")
	return client, nil
}
