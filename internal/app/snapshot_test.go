package app

import (
	"testing"
	"time"

	"github.com/grvbrk/nazrein_worker/internal/models"
	"github.com/grvbrk/nazrein_worker/internal/utils"
	"github.com/imagekit-developer/imagekit-go/api/uploader"
)

// prevSnapshot is a stand-in for the last row read back from ClickHouse.
func prevSnapshot() models.ClickhouseVideo {
	return models.ClickhouseVideo{
		VideoID:           "video-uuid",
		YoutubeID:         "dQw4w9WgXcQ",
		SnapshotTime:      time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC),
		Title:             "The old title",
		ImageSrc:          "https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg",
		Link:              "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
		TitleHash:         utils.HashString("The old title"),
		ImageEtag:         `"etag-old"`,
		ImageFileID:       "ik-file-1",
		ImageFilename:     "youtube_dQw4w9WgXcQ_video-uuid_1",
		ImageURL:          "https://ik.imagekit.io/n/one.jpg",
		ImageThumbnailURL: "https://ik.imagekit.io/n/tr:n-thumb/one.jpg",
		ImageHeight:       360,
		ImageWidth:        480,
		ImageSize:         12345,
		ImageFilepath:     "/testing/one.jpg",
		CreatedAt:         time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC),
	}
}

// Regression test for the title-only branch, which used to build its row by
// copying the previous snapshot — storing the OLD title alongside the NEW hash,
// stamped with the OLD timestamp, and with ImageSrc/Link crossed.
func TestBuildSnapshot_TitleOnlyChange(t *testing.T) {
	prev := prevSnapshot()
	observedAt := time.Date(2026, 2, 2, 12, 0, 0, 0, time.UTC)

	live := &models.OembedYTVideo{
		Title:        "A brand new title",
		ThumbnailURL: "https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg",
	}
	newHash := utils.HashString(live.Title)

	got := buildSnapshot(
		"video-uuid", "dQw4w9WgXcQ",
		"https://www.youtube.com/watch?v=dQw4w9WgXcQ",
		live, newHash, imageFieldsFromSnapshot(prev), observedAt,
	)

	if got.Title != live.Title {
		t.Errorf("Title = %q, want the newly observed title %q", got.Title, live.Title)
	}
	if got.Title == prev.Title {
		t.Error("Title was copied from the previous snapshot — this is the original bug")
	}
	if got.TitleHash != utils.HashString(got.Title) {
		t.Errorf("TitleHash does not hash the stored Title; hash and text disagree")
	}
	if !got.SnapshotTime.Equal(observedAt) {
		t.Errorf("SnapshotTime = %v, want the observation time %v", got.SnapshotTime, observedAt)
	}
	if !got.CreatedAt.Equal(observedAt) {
		t.Errorf("CreatedAt = %v, want the observation time %v", got.CreatedAt, observedAt)
	}
	if got.Link != "https://www.youtube.com/watch?v=dQw4w9WgXcQ" {
		t.Errorf("Link = %q, want the YouTube watch URL", got.Link)
	}
	if got.ImageSrc != live.ThumbnailURL {
		t.Errorf("ImageSrc = %q, want the source thumbnail URL %q", got.ImageSrc, live.ThumbnailURL)
	}

	// The thumbnail did not change, so the stored image must be carried over
	// verbatim rather than re-uploaded.
	if got.ImageFileID != prev.ImageFileID ||
		got.ImageURL != prev.ImageURL ||
		got.ImageThumbnailURL != prev.ImageThumbnailURL ||
		got.ImageEtag != prev.ImageEtag ||
		got.ImageHeight != prev.ImageHeight ||
		got.ImageWidth != prev.ImageWidth ||
		got.ImageSize != prev.ImageSize ||
		got.ImageFilepath != prev.ImageFilepath ||
		got.ImageFilename != prev.ImageFilename {
		t.Error("stored image fields were not carried over from the previous snapshot")
	}
}

func TestBuildSnapshot_FreshUpload(t *testing.T) {
	observedAt := time.Date(2026, 3, 3, 8, 30, 0, 0, time.UTC)
	live := &models.OembedYTVideo{
		Title:        "First observation",
		ThumbnailURL: "https://i.ytimg.com/vi/abc/hqdefault.jpg",
	}
	up := uploader.UploadResult{
		FileId:       "ik-file-2",
		Name:         "youtube_abc_v2_2",
		Url:          "https://ik.imagekit.io/n/two.jpg",
		ThumbnailUrl: "https://ik.imagekit.io/n/tr:n-thumb/two.jpg",
		Height:       720,
		Width:        1280,
		Size:         98765,
		FilePath:     "/testing/two.jpg",
	}

	got := buildSnapshot(
		"v2", "abc", "https://www.youtube.com/watch?v=abc",
		live, utils.HashString(live.Title),
		imageFieldsFromUpload(`"etag-new"`, up), observedAt,
	)

	if got.ImageFileID != up.FileId || got.ImageURL != up.Url {
		t.Error("fresh upload fields were not carried into the snapshot")
	}
	if got.ImageEtag != `"etag-new"` {
		t.Errorf("ImageEtag = %q, want the newly fetched etag", got.ImageEtag)
	}
	if got.ImageHeight != 720 || got.ImageWidth != 1280 {
		t.Errorf("dimensions = %dx%d, want 1280x720", got.ImageWidth, got.ImageHeight)
	}
	if got.ImageSize != 98765 {
		t.Errorf("ImageSize = %d, want 98765", got.ImageSize)
	}
}

// Whatever the source of the image fields, the observation fields must be
// identical — that symmetry is what stopped the branches drifting apart.
func TestBuildSnapshot_ObservationFieldsIndependentOfImageSource(t *testing.T) {
	observedAt := time.Date(2026, 4, 4, 10, 0, 0, 0, time.UTC)
	live := &models.OembedYTVideo{
		Title:        "Same observation",
		ThumbnailURL: "https://i.ytimg.com/vi/xyz/hqdefault.jpg",
	}
	hash := utils.HashString(live.Title)

	carried := buildSnapshot("v", "xyz", "https://www.youtube.com/watch?v=xyz",
		live, hash, imageFieldsFromSnapshot(prevSnapshot()), observedAt)

	uploaded := buildSnapshot("v", "xyz", "https://www.youtube.com/watch?v=xyz",
		live, hash, imageFieldsFromUpload(`"e"`, uploader.UploadResult{}), observedAt)

	if carried.Title != uploaded.Title ||
		carried.TitleHash != uploaded.TitleHash ||
		carried.Link != uploaded.Link ||
		carried.ImageSrc != uploaded.ImageSrc ||
		!carried.SnapshotTime.Equal(uploaded.SnapshotTime) ||
		!carried.CreatedAt.Equal(uploaded.CreatedAt) {
		t.Error("observation fields differ depending on where the image fields came from")
	}
}

func TestHashString_DistinguishesTitles(t *testing.T) {
	if utils.HashString("The old title") == utils.HashString("A brand new title") {
		t.Error("different titles hashed to the same value")
	}
	if utils.HashString("stable") != utils.HashString("stable") {
		t.Error("HashString is not deterministic")
	}
}
