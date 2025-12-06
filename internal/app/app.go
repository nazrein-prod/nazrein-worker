package app

import (
	"database/sql"
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
	config := config.NewConfig()

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
		msgCtx := models.MessageContext{Message: message}

		retryCount, err := w.RedisService.GetRetryCount(message.ID)
		if err != nil {
			msgCtx.Error = err
			failedContexts = append(failedContexts, msgCtx)
			continue
		}

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

		oEmbedVideo, err := w.YoutubeService.GetVideoDetails(url)
		if err != nil {
			msgCtx.Error = err
			failedContexts = append(failedContexts, msgCtx)
			continue
		}

		exists, err := w.CheckFirstUpload(videoID, oEmbedVideo)
		if err != nil {
			msgCtx.Error = err
			continue
		}

		if !exists {
			cacheKey := w.Config.ImageEtagPrefix + videoID
			newTitleHash := utils.HashString(oEmbedVideo.Title)
			newEtag, err := w.YoutubeService.GetImageEtag(oEmbedVideo.ThumbnailURL)
			if err != nil {
				msgCtx.Error = err
				continue
			}

			err = w.RedisClient.Set(w.Config.Ctx, cacheKey, newEtag, w.Config.ImageEtagTTL).Err()
			if err != nil {
				msgCtx.Error = err
				continue
			}

			// TODO: Cache the title hash too

			imageData, err := w.ImagekitService.DownloadAndUploadImage(videoID, youtubeID, oEmbedVideo.ThumbnailURL)
			if err != nil {
				msgCtx.Error = fmt.Errorf("failed to upload image to ImageKit: %w", err)
				failedContexts = append(failedContexts, msgCtx)
				continue
			}

			videoData := models.ClickhouseVideo{
				VideoID:           videoID,
				YoutubeID:         youtubeID,
				SnapshotTime:      time.Now(),
				Title:             oEmbedVideo.Title,
				ImageSrc:          oEmbedVideo.ThumbnailURL,
				Link:              url,
				TitleHash:         newTitleHash,
				ImageEtag:         newEtag,
				ImageFileID:       imageData.FileId,
				ImageFilename:     imageData.Name,
				ImageURL:          imageData.Url,
				ImageThumbnailURL: imageData.ThumbnailUrl,
				ImageHeight:       int32(imageData.Height),
				ImageWidth:        int32(imageData.Width),
				ImageSize:         imageData.Size,
				ImageFilepath:     imageData.FilePath,
				CreatedAt:         time.Now(),
			}

			msgCtx.VideoData = &videoData
			sucessfulVideos = append(sucessfulVideos, videoData)
			successfulMessageIDs = append(successfulMessageIDs, message.ID)

			continue
		}

		var newTitleHash uint64

		newImageEtag, err := w.HandleImageChange(videoID, oEmbedVideo)
		if err != nil {
			msgCtx.Error = err
			failedContexts = append(failedContexts, msgCtx)
			continue
		}

		if newImageEtag == "" {
			// Image didn't change
			// Check for title changes
			titleHash, err := w.DidTitleChange(videoID, oEmbedVideo)
			if err != nil {
				msgCtx.Error = err
				failedContexts = append(failedContexts, msgCtx)
				continue
			}

			newTitleHash = titleHash

			if newTitleHash == 0 {
				// No changes (Image and title)
				w.Logger.Printf("Acking message since no changes detected for video %s\n", youtubeID)
				err = w.RedisClient.XAck(w.Config.Ctx, w.Config.StreamName, w.Config.GroupName, message.ID).Err()
				if err != nil {
					w.Logger.Printf("Failed to XACK message %s: %v\n", message.ID, err)
				} else {
					w.Logger.Printf("Successfully processed and acknowledged message: %s\n", message.ID)
					w.RedisClient.Del(w.Config.Ctx, w.Config.RetryKeyPrefix+message.ID)
				}
				// successfulMessageIDs = append(successfulMessageIDs, message.ID)
				continue
			}

			// Title changed, Image didn't
			var chVideo models.ClickhouseVideo
			row := w.ClickhouseClient.QueryRow(w.Config.Ctx, `
				SELECT
					video_id,
					youtube_id,
					snapshot_time,
					title,
					image_src,
					link,
					title_hash,
					image_etag,
					image_file_id,
					image_filename,
					image_url,
					image_thumbnail_url,
					image_height,
					image_width,
					image_size,
					image_filepath,
					created_at
				FROM default.video_snapshots
				WHERE video_id = ?
				ORDER BY snapshot_time DESC
				LIMIT 1
			`, videoID)

			err = row.Scan(
				&chVideo.VideoID,
				&chVideo.YoutubeID,
				&chVideo.SnapshotTime,
				&chVideo.Title,
				&chVideo.ImageSrc,
				&chVideo.Link,
				&chVideo.TitleHash,
				&chVideo.ImageEtag,
				&chVideo.ImageFileID,
				&chVideo.ImageFilename,
				&chVideo.ImageURL,
				&chVideo.ImageThumbnailURL,
				&chVideo.ImageHeight,
				&chVideo.ImageWidth,
				&chVideo.ImageSize,
				&chVideo.ImageFilepath,
				&chVideo.CreatedAt,
			)

			if err != nil {
				if err == sql.ErrNoRows {
					msgCtx.Error = fmt.Errorf("no video details found in clickhouse: %w", err)
				} else {
					msgCtx.Error = fmt.Errorf("error fetching video details from clickhouse: %w", err)
				}
				failedContexts = append(failedContexts, msgCtx)
				continue
			}

			videoData := models.ClickhouseVideo{
				VideoID:           chVideo.VideoID,
				YoutubeID:         chVideo.YoutubeID,
				SnapshotTime:      chVideo.SnapshotTime,
				Title:             chVideo.Title,
				ImageSrc:          chVideo.ImageThumbnailURL,
				Link:              chVideo.ImageURL,
				TitleHash:         newTitleHash,
				ImageEtag:         chVideo.ImageEtag,
				ImageFileID:       chVideo.ImageFileID,
				ImageFilename:     chVideo.ImageFilename,
				ImageURL:          chVideo.ImageURL,
				ImageThumbnailURL: chVideo.ImageThumbnailURL,
				ImageHeight:       chVideo.ImageHeight,
				ImageWidth:        chVideo.ImageWidth,
				ImageSize:         chVideo.ImageSize,
				ImageFilepath:     chVideo.ImageFilepath,
				CreatedAt:         chVideo.CreatedAt,
			}

			sucessfulVideos = append(sucessfulVideos, videoData)
			continue
		}
		// Image and Title changed
		imageData, err := w.ImagekitService.DownloadAndUploadImage(videoID, youtubeID, oEmbedVideo.ThumbnailURL)
		if err != nil {
			msgCtx.Error = fmt.Errorf("failed to upload image to ImageKit: %w", err)
			failedContexts = append(failedContexts, msgCtx)
			continue
		}

		videoData := models.ClickhouseVideo{
			VideoID:           videoID,
			YoutubeID:         youtubeID,
			SnapshotTime:      time.Now(),
			Title:             oEmbedVideo.Title,
			ImageSrc:          oEmbedVideo.ThumbnailURL,
			Link:              url,
			TitleHash:         newTitleHash,
			ImageEtag:         newImageEtag,
			ImageFileID:       imageData.FileId,
			ImageFilename:     imageData.Name,
			ImageURL:          imageData.Url,
			ImageThumbnailURL: imageData.ThumbnailUrl,
			ImageHeight:       int32(imageData.Height),
			ImageWidth:        int32(imageData.Width),
			ImageSize:         imageData.Size,
			ImageFilepath:     imageData.FilePath,
			CreatedAt:         time.Now(),
		}

		msgCtx.VideoData = &videoData
		sucessfulVideos = append(sucessfulVideos, videoData)
		successfulMessageIDs = append(successfulMessageIDs, message.ID)
	}

	if len(successfulMessageIDs) > 0 {
		err := w.ClickhouseService.InsertVideos(sucessfulVideos)
		if err != nil {
			w.Logger.Printf("Failed to insert videos to ClickHouse: %v\n", err)
			return
		}

		for _, msgID := range successfulMessageIDs {
			err := w.RedisClient.XAck(w.Config.Ctx, w.Config.StreamName, w.Config.GroupName, msgID).Err()
			if err != nil {
				w.Logger.Printf("Failed to XACK message %s: %v\n", msgID, err)
			} else {
				w.Logger.Printf("Successfully processed and acknowledged message ID: %s\n", msgID)
				// Clean up retry counter
				w.RedisClient.Del(w.Config.Ctx, w.Config.RetryKeyPrefix+msgID)
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

func (w *Worker) HandleImageChange(videoID string, oEmbedVideo *models.OembedYTVideo) (string, error) {

	cacheKey := w.Config.ImageEtagPrefix + videoID
	cachedEtag, err := w.RedisClient.Get(w.Config.Ctx, cacheKey).Result()
	if err != nil && err != redis.Nil {
		return "", fmt.Errorf("redis get etag failed: %w", err)
	}

	newEtag, err := w.YoutubeService.GetImageEtag(oEmbedVideo.ThumbnailURL)
	if err != nil {
		return "", fmt.Errorf("failed to get image etag from thumbnail_url: %w", err)
	}

	// Case 1: Cache hit
	if cachedEtag != "" {
		// Same Etag -> Same image -> refresh TTL
		if newEtag == cachedEtag {
			err = w.RedisClient.Expire(w.Config.Ctx, cacheKey, w.Config.ImageEtagTTL).Err()
			if err != nil {
				return "", fmt.Errorf("error refreshing ttl for same image: %w", err)
			}
			return "", nil
		}

		// ETag changed -> update cache
		// Db updates will be batch handled in the processMessages function
		err = w.RedisClient.Set(w.Config.Ctx, cacheKey, newEtag, w.Config.ImageEtagTTL).Err()
		if err != nil {
			return "", fmt.Errorf("error refreshing ttl for different image: %w", err)
		}
		return newEtag, nil
	}

	// Case 2: Cache miss (cachedEtag == "")
	var dbEtag string
	err = w.ClickhouseClient.QueryRow(w.Config.Ctx, `
		SELECT image_etag
		FROM default.video_snapshots
		WHERE video_id = ?
		ORDER BY snapshot_time DESC
		LIMIT 1
	`, videoID).Scan(&dbEtag)

	if err != nil {
		return "", fmt.Errorf("clickhouse etag query failed: %w", err)
	}

	// Case 3: Found in DB and is the same as the fetched Etag
	// Means the redis cache expired
	// Just update the redis cache
	if dbEtag == newEtag {
		// db and current etag same -> update cache
		err = w.RedisClient.Set(w.Config.Ctx, cacheKey, dbEtag, w.Config.ImageEtagTTL).Err()
		if err != nil {
			return "", fmt.Errorf("error refreshing ttl for the same db etag: %w", err)
		}
		return "", nil
	}

	// Case 4: Different etag in db -> update cache and return the newEtag
	err = w.RedisClient.Set(w.Config.Ctx, cacheKey, newEtag, w.Config.ImageEtagTTL).Err()
	if err != nil {
		return "", fmt.Errorf("error refreshing ttl for different db etag: %w", err)
	}

	return newEtag, nil
}

func (w *Worker) DidTitleChange(videoID string, oEmbedVideo *models.OembedYTVideo) (uint64, error) {
	// TODO: Write the same cache -> db as imagechange
	newTitleHash := utils.HashString(oEmbedVideo.Title)

	var lastTitleHash uint64
	var title string
	err := w.ClickhouseClient.QueryRow(w.Config.Ctx, `
		SELECT title, title_hash
		FROM default.video_snapshots
		WHERE video_id = ?
		ORDER BY snapshot_time DESC
		LIMIT 1
	`, videoID).Scan(&title, &lastTitleHash)

	if err != nil {
		return 0, fmt.Errorf("failed to query latest snapshot: %w", err)
	}

	titleChanged := newTitleHash != lastTitleHash && oEmbedVideo.Title != title
	if titleChanged {
		w.Logger.Printf("Title changed for video %s\n", videoID)
		return newTitleHash, nil
	}

	return 0, nil
}

func (w *Worker) CheckFirstUpload(videoID string, oEmbedVideo *models.OembedYTVideo) (bool, error) {

	var exists bool
	err := w.ClickhouseClient.QueryRow(w.Config.Ctx, `
    SELECT EXISTS(
        SELECT 1
        FROM default.video_snapshots
        WHERE video_id = ?
    )
`, videoID).Scan(&exists)

	if err != nil {
		return false, fmt.Errorf("error querying exists for video. Err: %w", err)
	}

	return exists, nil
}
