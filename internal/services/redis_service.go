package services

import (
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/grvbrk/nazrein_worker/internal/config"
	"github.com/grvbrk/nazrein_worker/internal/models"
	"github.com/redis/go-redis/v9"
)

type RedisService struct {
	Client *redis.Client
	Config *config.Config
	Logger *slog.Logger
}

func NewRedisService(logger *slog.Logger, config *config.Config, client *redis.Client) *RedisService {
	return &RedisService{
		Client: client,
		Config: config,
		Logger: logger,
	}
}

func (rs *RedisService) CreateGroupAndStream() error {
	_, err := rs.Client.XGroupCreateMkStream(rs.Config.Ctx, rs.Config.StreamName, rs.Config.GroupName, "0").Result()

	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

func (rs *RedisService) LogPendingSummary() error {
	summary, err := rs.Client.XPending(rs.Config.Ctx, rs.Config.StreamName, rs.Config.GroupName).Result()
	if err != nil {
		return err
	}

	rs.Logger.Info("Pending messages summary",
		"total", summary.Count,
		"smallest_id", summary.Lower,
		"largest_id", summary.Higher,
	)

	return nil
}

func (rs *RedisService) ReadNewMessages() ([]redis.XStream, error) {
	return rs.Client.XReadGroup(rs.Config.Ctx, &redis.XReadGroupArgs{
		Group:    rs.Config.GroupName,
		Consumer: rs.Config.ConsumerID,
		Streams:  []string{rs.Config.StreamName, ">"},
		Count:    int64(rs.Config.BatchSize),
		Block:    rs.Config.BlockTime,
	}).Result()
}

func (rs *RedisService) ReadPendingMessages() ([]redis.XMessage, error) {
	pendingRes, err := rs.Client.XReadGroup(rs.Config.Ctx, &redis.XReadGroupArgs{
		Group:    rs.Config.GroupName,
		Consumer: rs.Config.ConsumerID,
		Streams:  []string{rs.Config.StreamName, "0"},
		Count:    int64(rs.Config.BatchSize),
	}).Result()

	if err != nil && err != redis.Nil {
		return nil, err
	}

	return pendingRes[0].Messages, nil
}

func (rs *RedisService) GetRetryCount(messageID string) (int, error) {
	retryKey := rs.Config.RetryKeyPrefix + messageID
	val, err := rs.Client.Get(rs.Config.Ctx, retryKey).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("error getting retry count from redis: %w", err)
	}

	count, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("error converting string count var to int: %w", err)
	}

	return count, nil
}

func (rs *RedisService) HandleRetries(msgCtx models.MessageContext) {
	retryCount := msgCtx.RetryCount + 1
	retryKey := rs.Config.RetryKeyPrefix + msgCtx.Message.ID

	err := rs.Client.Set(rs.Config.Ctx, retryKey, retryCount, 24*time.Hour).Err()
	if err != nil {
		rs.Logger.Error("Failed to set retry count for message", "message_id", msgCtx.Message.ID, "err", err)
	}

	// calc exponential backoff delay
	backoffDelay := time.Duration(retryCount*retryCount) * time.Second

	rs.Logger.Warn("Message failed, retrying",
		"message_id", msgCtx.Message.ID,
		"attempt", retryCount,
		"max_retries", rs.Config.MaxRetries,
		"err", msgCtx.Error,
		"backoff", backoffDelay,
	)

	// we'll just not ack the message here so it gets reprocessed
}

func (rs *RedisService) HandleFailure(msgCtx models.MessageContext) {
	rs.Logger.Error("Message exceeded max retries, moving to dead letter stream",
		"message_id", msgCtx.Message.ID,
		"max_retries", rs.Config.MaxRetries,
		"err", msgCtx.Error,
	)

	deadLetterData := map[string]interface{}{
		"original_stream":     rs.Config.StreamName,
		"original_message_id": msgCtx.Message.ID,
		"retry_count":         msgCtx.RetryCount,
		"error":               msgCtx.Error.Error(),
		"failed_at":           time.Now().Unix(),
	}

	for k, v := range msgCtx.Message.Values {
		deadLetterData[k] = v
	}

	_, err := rs.Client.XAdd(rs.Config.Ctx, &redis.XAddArgs{
		Stream: rs.Config.DeadLetterStreamName,
		Values: deadLetterData,
	}).Result()

	if err != nil {
		rs.Logger.Error("Failed to add message to dead letter stream", "message_id", msgCtx.Message.ID, "err", err)
		return
	}

	err = rs.Client.XAck(rs.Config.Ctx, rs.Config.StreamName, rs.Config.GroupName, msgCtx.Message.ID).Err()
	if err != nil {
		rs.Logger.Error("Failed to XACK failed message", "message_id", msgCtx.Message.ID, "err", err)
	} else {
		rs.Logger.Info("Moved message to dead letter stream and acknowledged", "message_id", msgCtx.Message.ID)
	}

	rs.Client.Del(rs.Config.Ctx, rs.Config.RetryKeyPrefix+msgCtx.Message.ID)
}
