package operation

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDownloader_DownloadBytes(t *testing.T) {
	config := getConfig1()

	uploader := NewUploader(config)

	data := []byte("test1")
	err := uploader.UploadData(data, "test1")
	assert.NoError(t, err)

	// downloader
	downloader := NewDownloader(config)
	_data, err := downloader.DownloadBytes("test1")
	assert.NoError(t, err)
	assert.Equal(t, data, _data)
}

func TestDownloader_DownloadRaw(t *testing.T) {
	config := getConfig1()

	uploader := NewUploader(config)

	data := []byte("test1")
	err := uploader.UploadData(data, "test1")
	assert.NoError(t, err)

	// downloader
	downloader := NewDownloader(config)
	body, err := downloader.DownloadRaw("test1", nil)
	assert.NoError(t, err)

	buf := new(bytes.Buffer)
	buf.ReadFrom(body)

	// get a byte slice from bytes.Buffer
	_data := buf.Bytes()
	assert.Equal(t, data, _data)
}

func TestDownloader_DownloadRangeReader(t *testing.T) {
	config := getConfig1()

	uploader := NewUploader(config)

	data := []byte("test1")
	err := uploader.UploadData(data, "test1")
	assert.NoError(t, err)

	// downloader
	downloader := NewDownloader(config)
	l, _, err := downloader.DownloadRangeReader("test1", 0, 1)
	assert.NoError(t, err)
	assert.Equal(t, l, 1)
	// assert.Equal(t, data, body)
}

func TestDownloader_DownloadFile(t *testing.T) {
	config := getConfig1()

	uploader := NewUploader(config)

	data := []byte("test1")
	err := uploader.UploadData(data, "test1")
	assert.NoError(t, err)

	// downloader
	downloader := NewDownloader(config)
	file, err := downloader.DownloadFile("test1", "test.txt")
	assert.NoError(t, err)
	defer file.Close()
	// assert.Equal(t, data, body)
}
