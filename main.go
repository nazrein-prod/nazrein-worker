package main

import (
	"github.com/grvbrk/nazrein_worker/internal/app"
	"github.com/redis/go-redis/v9"
)

func main() {

	worker, err := app.NewWorker()
	if err != nil {
		worker.Logger.Fatal("Error creating worker:", err)
	}

	defer func() {
		if err := worker.RedisClient.Close(); err != nil {
			worker.Logger.Println("Error closing redis client", err)
		}
	}()

	// Create consumer group
	err = worker.RedisService.CreateGroupAndStream()
	if err != nil {
		worker.Logger.Println("Failed to create consumer group", err)
		return
	}

	err = worker.RedisService.LogPendingSummary()
	if err != nil {
		worker.Logger.Println("Error checking XPENDING:", err)
		return
	}

	// first read any pending messages (in case of restarts)
	messages, err := worker.RedisService.ReadPendingMessages()
	if err != nil {
		worker.Logger.Println("Error reading pending:", err)
		return
	}

	if len(messages) > 0 {
		worker.Logger.Println("Processing pending messages...")
		worker.Logger.Println("Found", len(messages), "pending messages")
		worker.ProcessMessages(messages)
	}

	worker.Logger.Println("No pending messages, listening for new ones...")

	// Code reaches here if there are no pending messages
	for {
		newRes, err := worker.RedisService.ReadNewMessages() // this blocks for config.Blocktime duration
		if err == redis.Nil || len(newRes) == 0 || len(newRes[0].Messages) == 0 {
			worker.Logger.Println("No new messages, waiting for new ones...")
			continue
		}
		worker.ProcessMessages(newRes[0].Messages)
	}
}
