package services

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/grvbrk/nazrein_worker/internal/config"
	"github.com/imagekit-developer/imagekit-go"
	"github.com/imagekit-developer/imagekit-go/api/uploader"
)

type ImagekitService struct {
	Logger         *log.Logger
	Config         *config.Config
	HttpClient     *http.Client
	ImageKitClient *imagekit.ImageKit
}

func NewImagekitService(logger *log.Logger, config *config.Config, httpClient *http.Client, imageKitClient *imagekit.ImageKit) *ImagekitService {
	return &ImagekitService{
		Logger:         logger,
		Config:         config,
		HttpClient:     httpClient,
		ImageKitClient: imageKitClient,
	}
}

func (is *ImagekitService) DownloadAndUploadImage(videoID, youtubeID, imageURL string) (uploader.UploadResult, error) {

	timestamp := time.Now().Unix()
	filename := fmt.Sprintf("youtube_%s_%s_%d", youtubeID, videoID, timestamp)

	uploadResp, err := is.ImageKitClient.Uploader.Upload(is.Config.Ctx, imageURL, uploader.UploadParam{
		FileName: filename,
	})

	if err != nil {
		return uploader.UploadResult{}, fmt.Errorf("failed to upload image: %w", err)
	}

	if uploadResp.StatusCode != http.StatusOK {
		return uploader.UploadResult{}, fmt.Errorf("ImageKit upload failed: %s", uploadResp.ParseError())
	}

	is.Logger.Println("ImageKit upload successful")

	return uploadResp.Data, nil
}
