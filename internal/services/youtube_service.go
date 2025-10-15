package services

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"

	"github.com/grvbrk/nazrein_worker/internal/config"
	"github.com/grvbrk/nazrein_worker/internal/models"
)

type YoutubeService struct {
	Logger     *log.Logger
	Config     *config.Config
	HttpClient *http.Client
}

func NewYoutubeService(logger *log.Logger, config *config.Config, httpClient *http.Client) *YoutubeService {
	return &YoutubeService{
		Logger:     logger,
		Config:     config,
		HttpClient: httpClient,
	}
}

func (ys *YoutubeService) GetVideoDetails(videoURL string) (*models.OembedYTVideo, error) {
	oembedURL := fmt.Sprintf("https://www.youtube.com/oembed?url=%s&format=json", url.QueryEscape(videoURL))

	resp, err := ys.HttpClient.Get(oembedURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch video details: %w", err)
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			ys.Logger.Printf("failed to close oEmbed response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("YouTube oEmbed API returned status %d", resp.StatusCode)
	}

	var video models.OembedYTVideo
	if err := json.NewDecoder(resp.Body).Decode(&video); err != nil {
		return nil, fmt.Errorf("failed to decode oEmbed response: %w", err)
	}

	return &video, nil
}

// Get md5 hash of the downloaded image by doing a fetch call to the image URL
func (ys *YoutubeService) DownloadAndMD5HashImage(imageURL string) (string, error) {
	resp, err := ys.HttpClient.Get(imageURL)
	if err != nil {
		return "", fmt.Errorf("failed to download image: %w", err)
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			ys.Logger.Printf("failed to close imageurl (for md5) client: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("image download returned status %d", resp.StatusCode)
	}

	// Stream hash computation -> memory efficient for large images
	hasher := md5.New()
	_, err = io.Copy(hasher, resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to hash image content: %w", err)
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func (ys *YoutubeService) GetImageEtag(imageURL string) (string, error) {
	resp, err := ys.HttpClient.Head(imageURL)
	if err != nil {
		return "", fmt.Errorf("failed to get image etag: %w", err)
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			ys.Logger.Printf("failed to close imageurl (for etag) client: %v", err)
		}
	}()

	etag := resp.Header.Get("ETag")
	if etag == "" {
		return "", fmt.Errorf("no ETag header found in response")
	}

	return etag, nil
}
