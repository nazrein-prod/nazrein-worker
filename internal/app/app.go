package app

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grvbrk/nazrein_worker/internal/config"
	"github.com/grvbrk/nazrein_worker/internal/db"
	"github.com/grvbrk/nazrein_worker/internal/models"
	"github.com/grvbrk/nazrein_worker/internal/services"
	"github.com/grvbrk/nazrein_worker/internal/utils"
	"github.com/imagekit-developer/imagekit-go"
	"github.com/redis/go-redis/v9"
)

type Worker struct {
	Logger            *log.Logger
	ImageKit          *imagekit.ImageKit
	RedisClient       *redis.Client
	ClickhouseClient  driver.Conn
	HttpClient        *http.Client
	Config            *config.Config
	RedisService      *services.RedisService
	YoutubeService    *services.YoutubeService
	ClickhouseService *services.ClickhouseService
	ImagekitService   *services.ImagekitService
}

func NewWorker() (*Worker, error) {
	logger := log.New(os.Stdout, "LOGGING: ", log.Ldate|log.Ltime)

	imageKitClient, err := db.ConnectImageKit()
	if err != nil {
		fmt.Println("Error connecting to ImageKit:", err)
		return nil, err
	}

	redisClient, err := db.ConnectRedis()
	if err != nil {
		fmt.Println("Error connecting to Redis:", err)
		return nil, err
	}

	chConn, err := db.ConnectClickhouse()
	if err != nil {
		fmt.Println("Error connecting to Clickhouse:", err)
		return nil, err
	}

	httpClient := &http.Client{
		Timeout: 15 * time.Second,
	}

	config := config.NewConfig()

	youtubeService := services.NewYoutubeService(logger, config, httpClient)
	clickhouseService := services.NewClickhouseService(logger, config, chConn)
	redisService := services.NewRedisService(logger, config, redisClient)
	imagekitService := services.NewImagekitService(logger, config, httpClient, imageKitClient)

	worker := Worker{
		Logger:            logger,
		ImageKit:          imageKitClient,
		RedisClient:       redisClient,
		ClickhouseClient:  chConn,
		HttpClient:        httpClient,
		Config:            config,
		RedisService:      redisService,
		YoutubeService:    youtubeService,
		ClickhouseService: clickhouseService,
		ImagekitService:   imagekitService,
	}

	return &worker, nil
}

func (w *Worker) ProcessMessages(messages []redis.XMessage) {

	var sucessfulVideos []models.ClickhouseVideo
	var successfulMessageIDs []string
	var failedContexts []models.MessageContext

	for _, message := range messages {
		// w.Logger.Printf("Processing message ID: %s\n", message.ID)

		msgCtx := models.MessageContext{Message: message}

		retryCount, _ := w.RedisService.GetRetryCount(message.ID)
		msgCtx.RetryCount = retryCount

		videoID, ok := message.Values["id"].(string)
		if !ok || videoID == "" {
			msgCtx.Error = fmt.Errorf("invalid or missing 'id' in message")
			failedContexts = append(failedContexts, msgCtx)
			continue
		}

		url, ok := message.Values["link"].(string)
		if !ok || url == "" {
			msgCtx.Error = fmt.Errorf("invalid or missing 'link' in message")
			failedContexts = append(failedContexts, msgCtx)
			continue
		}

		youtubeID, ok := message.Values["youtube_id"].(string)
		if !ok || youtubeID == "" {
			msgCtx.Error = fmt.Errorf("invalid or missing 'youtube_id' in message")
			failedContexts = append(failedContexts, msgCtx)
			continue
		}

		newVideo, err := w.YoutubeService.GetVideoDetails(url)
		if err != nil {
			msgCtx.Error = err
			failedContexts = append(failedContexts, msgCtx)
			continue
		}

		// titleHash, imageEtag should only be used when shouldInsert is true
		// Kind of an anti pattern
		shouldInsert, newTitleHash, newImageEtag, err := w.ShouldInsertSnapshot(videoID, newVideo)
		if err != nil {
			msgCtx.Error = fmt.Errorf("failed to check if snapshot needed: %w", err)
			failedContexts = append(failedContexts, msgCtx)
			continue
		}

		if !shouldInsert {
			w.Logger.Printf("No changes detected for video %s, skipping insertion\n", youtubeID)
			w.Logger.Println("Acking message since no changes detected")
			err = w.RedisClient.XAck(w.Config.Ctx, w.Config.StreamName, w.Config.GroupName, message.ID).Err()
			if err != nil {
				w.Logger.Printf("Failed to XACK message %s: %v\n", message.ID, err)
			} else {
				w.Logger.Printf("Successfully processed and acknowledged message: %s\n", message.ID)
				w.RedisClient.Del(w.Config.Ctx, w.Config.RetryKeyPrefix+message.ID)
			}
			continue
		}

		// shouldInsert is true ==> Upload image to ImageKit
		// No matter what changed, image or title, we still upload the image
		// TODO: Avoid uploading image if it's not changed
		// GOOGLE: Should we offload the storing of image part to another stream?
		// GOOGLE: Also, what should we do if this fails?
		imageData, err := w.ImagekitService.DownloadAndUploadImage(videoID, youtubeID, newVideo.ThumbnailURL)
		if err != nil {
			msgCtx.Error = fmt.Errorf("failed to upload image to ImageKit: %w", err)
			failedContexts = append(failedContexts, msgCtx)
			continue
		}

		videoData := models.ClickhouseVideo{
			VideoID:           videoID,
			YoutubeID:         youtubeID,
			SnapshotTime:      time.Now(),
			Title:             newVideo.Title,
			ImageSrc:          newVideo.ThumbnailURL,
			Link:              url,
			TitleHash:         newTitleHash,
			ImageEtag:         newImageEtag,
			ImageFileID:       imageData.FileId,
			ImageFilename:     imageData.Name,
			ImageURL:          imageData.Url,
			ImageThumbnailURL: imageData.ThumbnailUrl,
			ImageHeight:       imageData.Height,
			ImageWidth:        imageData.Width,
			ImageSize:         imageData.Size,
			ImageFilepath:     imageData.FilePath,
			CreatedAt:         time.Now(),
		}

		msgCtx.VideoData = &videoData
		sucessfulVideos = append(sucessfulVideos, videoData)
		successfulMessageIDs = append(successfulMessageIDs, message.ID)
	}

	if len(sucessfulVideos) > 0 {
		err := w.ClickhouseService.InsertVideos(sucessfulVideos)
		if err != nil {
			w.Logger.Printf("Failed to insert videos to ClickHouse: %v\n", err)
			return
		}

		for _, id := range successfulMessageIDs {
			w.Logger.Println("Acking message", id)
			err = w.RedisClient.XAck(w.Config.Ctx, w.Config.StreamName, w.Config.GroupName, id).Err()
			if err != nil {
				w.Logger.Printf("Failed to XACK message %s: %v\n", id, err)
			} else {
				w.Logger.Printf("Successfully processed and acknowledged message ID: %s\n", id)
				// Clean up retry counter
				w.RedisClient.Del(w.Config.Ctx, w.Config.RetryKeyPrefix+id)
			}
		}
	}

	for _, failedCtx := range failedContexts {
		if failedCtx.RetryCount < w.Config.MaxRetries {
			w.RedisService.HandleRetries(failedCtx)
		} else {
			w.RedisService.HandleFailure(failedCtx)
		}
	}

}

func (w *Worker) ShouldInsertSnapshot(videoID string, newVideo *models.OembedYTVideo) (bool, uint64, string, error) {
	// The thinking here is like this:
	// 1. make the title hash no matter what
	// 2. Check if the image changed
	// 3. If the image has changed, dont care if the title has changed or not - we return the new title hash and image etag.
	// 4. If the image has not changed, NOW CHECK if the title has changed or not - we don't want to discard title changes if image hasn't changed
	// 5. If the title has changed, we return the new image and title hashes
	// THEREFORE, we return the image and title hashes always.
	// These retuned hashes will be useful only when the boolean value is true, otherwise they will be 0 and "".

	newTitleHash := utils.HashString(newVideo.Title)

	// Check image etag with previous image etag from redis
	imageChanged, imageEtag, err := w.checkImageChange(videoID, newVideo.ThumbnailURL)
	if err != nil {
		return false, 0, "", fmt.Errorf("failed to check image change: %w", err)
	}

	// If image changed, we return the new image etag and title hash immediately
	if imageChanged {
		w.Logger.Printf("Image changed for video %s\n", videoID)
		return true, newTitleHash, imageEtag, nil
	}

	// Image didn't change, check if title changed by getting it from clickhouse
	// GOOGLE: Should we store the title hash too in redis?
	var lastTitleHash uint64
	err = w.ClickhouseClient.QueryRow(w.Config.Ctx, `
		SELECT title_hash
		FROM default.video_snapshots
		WHERE video_id = ?
		ORDER BY snapshot_time DESC
		LIMIT 1
	`, videoID).Scan(&lastTitleHash)

	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			w.Logger.Printf("No previous snapshot found for video %s, inserting first snapshot\n", videoID)
			return true, newTitleHash, imageEtag, nil
		}
		return false, 0, "", fmt.Errorf("failed to query latest snapshot: %w", err)
	}

	titleChanged := newTitleHash != lastTitleHash

	if titleChanged {
		w.Logger.Printf("Title changed for video %s\n", videoID)
		return true, newTitleHash, imageEtag, nil
	}

	return false, 0, "", nil
}

// Returns true if image changed, image etag string, and error
func (w *Worker) checkImageChange(videoID string, imageURL string) (bool, string, error) {
	cacheKey := w.Config.ImageEtagPrefix + videoID

	// Do a fetch call to youtube's image url endpoint to get the image etag
	currentEtag, err := w.YoutubeService.GetImageEtag(imageURL)
	if err != nil {
		return false, "", fmt.Errorf("failed to get image MD5: %w", err)
	}

	// fmt.Printf("Current fetched Etag: %v\n", currentEtag)

	// Check cached etag of the same image
	cachedEtag, err := w.RedisClient.Get(w.Config.Ctx, cacheKey).Result()
	if err == redis.Nil {
		// First time - cache the etag
		// fmt.Printf("First time fetching image etag - cache the etag: %v", currentEtag)
		err = w.RedisClient.Set(w.Config.Ctx, cacheKey, currentEtag, w.Config.ImageEtagTTL).Err()
		if err != nil {
			fmt.Printf("Warning: failed to cache etag for video %s: %v\n", videoID, err)
		}
		return true, currentEtag, nil // First time, consider as changed
	} else if err != nil {
		return false, "", fmt.Errorf("redis error: %w", err)
	}

	// fmt.Printf("Cached Etag: %v\n", cachedEtag)
	imageChanged := currentEtag != cachedEtag

	if imageChanged {
		// Update cache with new etag
		err = w.RedisClient.Set(w.Config.Ctx, cacheKey, currentEtag, w.Config.ImageEtagTTL).Err()
		if err != nil {
			fmt.Printf("failed to update etag for video %s: %v\n", videoID, err)
		}
		return true, currentEtag, nil
	}

	return false, "", nil
}
