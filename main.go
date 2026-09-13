package main

import (
	"log/slog"
	"os"

	"github.com/grvbrk/nazrein_worker/internal/app"
	"github.com/redis/go-redis/v9"
)

func main() {

	worker, err := app.NewWorker()
	if err != nil {
		slog.Error("Failed to create worker", "err", err)
		os.Exit(1)
	}

	slog.SetDefault(worker.Logger)

	defer func() {
		if err := worker.RedisClient.Close(); err != nil {
			worker.Logger.Warn("Error closing redis client", "err", err)
		}
	}()

	// Create consumer group
	err = worker.RedisService.CreateGroupAndStream()
	if err != nil {
		worker.Logger.Error("Failed to create consumer group", "err", err)
		return
	}

	err = worker.RedisService.LogPendingSummary()
	if err != nil {
		worker.Logger.Error("Error checking XPENDING", "err", err)
		return
	}

	// first read any pending messages (in case of restarts)
	messages, err := worker.RedisService.ReadPendingMessages()
	if err != nil {
		worker.Logger.Error("Error reading pending", "err", err)
		return
	}

	if len(messages) > 0 {
		worker.Logger.Info("Processing pending messages", "count", len(messages))
		worker.ProcessMessages(messages)
	}

	worker.Logger.Info("No pending messages, listening for new ones...")

	// Code reaches here if there are no pending messages
	for {
		newRes, err := worker.RedisService.ReadNewMessages() // this blocks for config.Blocktime duration
		if err == redis.Nil || len(newRes) == 0 || len(newRes[0].Messages) == 0 {
			continue
		}
		worker.ProcessMessages(newRes[0].Messages)
	}
}
