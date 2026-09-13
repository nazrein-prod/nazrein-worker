package app

import (
	"time"

	"github.com/grvbrk/nazrein_worker/internal/models"
	"github.com/imagekit-developer/imagekit-go/api/uploader"
)

type imageFields struct {
	Etag         string
	FileID       string
	Filename     string
	URL          string
	ThumbnailURL string
	Height       int32
	Width        int32
	Size         uint64
	Filepath     string
}

// imageFieldsFromUpload describes a thumbnail that was just mirrored to ImageKit.
func imageFieldsFromUpload(etag string, up uploader.UploadResult) imageFields {
	return imageFields{
		Etag:         etag,
		FileID:       up.FileId,
		Filename:     up.Name,
		URL:          up.Url,
		ThumbnailURL: up.ThumbnailUrl,
		Height:       int32(up.Height),
		Width:        int32(up.Width),
		Size:         up.Size,
		Filepath:     up.FilePath,
	}
}

// imageFieldsFromSnapshot reuses the stored image of an earlier snapshot, for the
// case where the thumbnail has not changed since it was taken.
func imageFieldsFromSnapshot(prev models.ClickhouseVideo) imageFields {
	return imageFields{
		Etag:         prev.ImageEtag,
		FileID:       prev.ImageFileID,
		Filename:     prev.ImageFilename,
		URL:          prev.ImageURL,
		ThumbnailURL: prev.ImageThumbnailURL,
		Height:       prev.ImageHeight,
		Width:        prev.ImageWidth,
		Size:         prev.ImageSize,
		Filepath:     prev.ImageFilepath,
	}
}

// buildSnapshot assembles the ClickHouse row for a single observation of a video.
func buildSnapshot(
	videoID string,
	youtubeID string,
	link string,
	video *models.OembedYTVideo,
	titleHash uint64,
	img imageFields,
	observedAt time.Time,
) models.ClickhouseVideo {
	return models.ClickhouseVideo{
		VideoID:           videoID,
		YoutubeID:         youtubeID,
		SnapshotTime:      observedAt,
		Title:             video.Title,
		ImageSrc:          video.ThumbnailURL,
		Link:              link,
		TitleHash:         titleHash,
		ImageEtag:         img.Etag,
		ImageFileID:       img.FileID,
		ImageFilename:     img.Filename,
		ImageURL:          img.URL,
		ImageThumbnailURL: img.ThumbnailURL,
		ImageHeight:       img.Height,
		ImageWidth:        img.Width,
		ImageSize:         img.Size,
		ImageFilepath:     img.Filepath,
		CreatedAt:         observedAt,
	}
}
