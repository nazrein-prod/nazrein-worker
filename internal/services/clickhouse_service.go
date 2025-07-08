package services

import (
	"fmt"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/grvbrk/trackyt_worker/internal/config"
	"github.com/grvbrk/trackyt_worker/internal/models"
)

type ClickhouseService struct {
	Logger           *log.Logger
	Config           *config.Config
	ClickhouseClient driver.Conn
}

func NewClickhouseService(logger *log.Logger, config *config.Config, clickhouseClient driver.Conn) *ClickhouseService {
	return &ClickhouseService{
		Logger:           logger,
		Config:           config,
		ClickhouseClient: clickhouseClient,
	}
}

func (cs *ClickhouseService) InsertVideos(videos []models.ClickhouseVideo) error {

	batch, err := cs.ClickhouseClient.PrepareBatch(cs.Config.Ctx, "INSERT INTO default.video_snapshots (video_id, youtube_id, snapshot_time, title, image_src, link, title_hash, image_etag, image_file_id, image_filename, image_url, image_thumbnail_url, image_height, image_width, image_size, image_filepath, created_at)")
	if err != nil {
		return fmt.Errorf("error preparing ClickHouse batch: %w", err)
	}
	defer batch.Close()

	for _, video := range videos {
		err = batch.Append(video.VideoID, video.YoutubeID, video.SnapshotTime, video.Title, video.ImageSrc, video.Link, video.TitleHash, video.ImageEtag, video.ImageFileID, video.ImageFilename, video.ImageURL, video.ImageThumbnailURL, video.ImageHeight, video.ImageWidth, video.ImageSize, video.ImageFilepath, video.CreatedAt)
		if err != nil {
			return fmt.Errorf("error appending to ClickHouse batch: %w", err)
		}
	}

	err = batch.Send()
	if err != nil {
		return fmt.Errorf("error sending ClickHouse batch: %w", err)
	}

	return nil
}
