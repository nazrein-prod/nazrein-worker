package services

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/grvbrk/nazrein_worker/internal/config"
	"github.com/grvbrk/nazrein_worker/internal/models"
	"github.com/redis/go-redis/v9"
)

type RedisService struct {
	Client *redis.Client
	Config *config.Config
	Logger *log.Logger
}

func NewRedisService(logger *log.Logger, config *config.Config, client *redis.Client) *RedisService {
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

	rs.Logger.Printf("Total pending: %d, smallest ID: %s, largest ID: %s\n",
		summary.Count, summary.Lower, summary.Higher)

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
		return 0, err
	}

	count, err := strconv.Atoi(val)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (rs *RedisService) HandleRetries(msgCtx models.MessageContext) {
	retryCount := msgCtx.RetryCount + 1
	retryKey := rs.Config.RetryKeyPrefix + msgCtx.Message.ID

	err := rs.Client.Set(rs.Config.Ctx, retryKey, retryCount, 24*time.Hour).Err()
	if err != nil {
		rs.Logger.Printf("Failed to set retry count for message %s: %v\n", msgCtx.Message.ID, err)
	}

	// calc exponential backoff delay
	backoffDelay := time.Duration(retryCount*retryCount) * time.Second

	rs.Logger.Printf("Message %s failed (attempt %d/%d): %v. Retrying in %v\n",
		msgCtx.Message.ID, retryCount, rs.Config.MaxRetries, msgCtx.Error, backoffDelay)

	// we'll just not ack the message here so it gets reprocessed
}

func (rs *RedisService) HandleFailure(msgCtx models.MessageContext) {
	fmt.Printf("Message %s exceeded max retries (%d). Moving to dead letter stream. Error: %v\n",
		msgCtx.Message.ID, rs.Config.MaxRetries, msgCtx.Error)

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
		fmt.Printf("Failed to add message to dead letter stream: %v\n", err)
		return
	}

	err = rs.Client.XAck(rs.Config.Ctx, rs.Config.StreamName, rs.Config.GroupName, msgCtx.Message.ID).Err()
	if err != nil {
		rs.Logger.Printf("Failed to XACK failed message %s: %v\n", msgCtx.Message.ID, err)
	} else {
		rs.Logger.Printf("Moved message %s to dead letter stream and acknowledged\n", msgCtx.Message.ID)
	}

	rs.Client.Del(rs.Config.Ctx, rs.Config.RetryKeyPrefix+msgCtx.Message.ID)
}
