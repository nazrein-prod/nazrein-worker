package models

import (
	"time"

	"github.com/redis/go-redis/v9"
)

type ClickhouseVideo struct {
	VideoID           string    `db:"video_id"`
	YoutubeID         string    `db:"youtube_id"`
	SnapshotTime      time.Time `db:"snapshot_time"`
	Title             string    `db:"title"`
	ImageSrc          string    `db:"image_src"`
	Link              string    `db:"link"`
	TitleHash         uint64    `db:"title_hash"`
	ImageEtag         string    `db:"image_etag"`
	ImageFileID       string    `db:"image_file_id"`
	ImageFilename     string    `db:"image_filename"`
	ImageURL          string    `db:"image_url"`
	ImageThumbnailURL string    `db:"image_thumbnail_url"`
	ImageHeight       int       `db:"image_height"`
	ImageWidth        int       `db:"image_width"`
	ImageSize         uint64    `db:"image_size"`
	ImageFilepath     string    `db:"image_filepath"`
	CreatedAt         time.Time `db:"created_at"`
}

type RedisMessage struct {
	ID        string `json:"id"`
	Link      string `json:"link"`
	YoutubeID string `json:"youtube_id"`
}

type OembedYTVideo struct {
	Title        string `json:"title"`
	ThumbnailURL string `json:"thumbnail_url"`
	AuthorName   string `json:"author_name"`
	HTML         string `json:"html"`
}

type MessageContext struct {
	Message    redis.XMessage
	RetryCount int
	VideoData  *ClickhouseVideo
	Error      error
}
