package operation

import (
	"io"
	"net/http"
	"os"
)

type clusterDownloader interface {
	downloadRaw(key string, headers http.Header) (io.ReadCloser, error)
	downloadFile(key, path string) (f *os.File, err error)
	downloadBytes(key string) (data []byte, err error)
	downloadRangeBytes(key string, offset, size int64) (l int64, data []byte, err error)
	downloadRangeReader(key string, offset, size int64) (l int64, reader io.ReadCloser, err error)
}

// Downloader 下载器
type Downloader struct {
	clusterDownloader
}

// NewDownloader 根据配置创建下载器
func NewDownloader(c *Config) *Downloader {
	newSingleClusterDownloader(c)
	return &Downloader{}
}

// DownloadCheck 检查文件
func (d *Downloader) DownloadCheck(key string) (l int64, err error) {
	l, _, err = d.DownloadRangeBytes(key, -1, 4)
	return
}

// 与七牛SDK不同，这里不支持headers,也不支持返回http.Response, 支持返回io.Reader
// DownloadRaw 使用给定的 HTTP Header 请求下载接口，并直接获得 http.Response 响应
func (d *Downloader) DownloadRaw(key string, headers http.Header) (io.ReadCloser, error) {
	return d.downloadRaw(key, headers)
}

// DownloadRangeReader 下载指定对象的指定范围为Reader
func (d *Downloader) DownloadRangeReader(key string, offset, size int64) (l int64, reader io.ReadCloser, err error) {
	return d.downloadRangeReader(key, offset, size)
}

// DownloadRangeBytes 下载指定对象的指定范围到内存中
func (d *Downloader) DownloadRangeBytes(key string, offset, size int64) (l int64, data []byte, err error) {
	return d.downloadRangeBytes(key, offset, size)
}

// DownloadBytes 下载指定对象到内存中
func (d *Downloader) DownloadBytes(key string) (data []byte, err error) {
	return d.downloadBytes(key)
}

// DownloadFile 下载指定对象到文件里
func (d *Downloader) DownloadFile(key, path string) (f *os.File, err error) {
	return d.downloadFile(key, path)
}
