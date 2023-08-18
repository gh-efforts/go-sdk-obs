package operation

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	obs "github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

type singleClusterDownloader struct {
	bucket        string
	partSize      int64
	upConcurrency int
	client        *obs.ObsClient
}

func newSingleClusterDownloader(c *Config) *singleClusterDownloader {
	lister := singleClusterDownloader{}
	obsClient, err := obs.New(c.Ak, c.Sk, c.EndPoint)

	if err != nil {
		fmt.Printf("Create obsClient error, errMsg: %s", err.Error())
	}

	lister.client = obsClient
	lister.bucket = c.Bucket

	if c.UpConcurrency <= 0 {
		lister.upConcurrency = 20
	}
	part := c.PartSize * 1024 * 1024
	if part < 4*1024*1024 {
		part = 4 * 1024 * 1024
	}
	lister.partSize = part
	return &lister
}

func (d *singleClusterDownloader) downloadRawInner(key string, headers http.Header) (resp io.ReadCloser, err error) {

	if d.client == nil {
		return nil, errors.New("obsclient is nil")
	}

	input := &obs.GetObjectInput{}
	input.Bucket = d.bucket
	input.Key = key
	output, err := d.client.GetObject(input)

	if err == nil {
		return output.Body, nil
	}
	return nil, err
}

func (d *singleClusterDownloader) downloadRaw(key string, headers http.Header) (resp io.ReadCloser, err error) {
	for i := 0; i < 3; i++ {
		resp, err = d.downloadRawInner(key, headers)
		if err == nil {
			return
		}
	}
	return
}

func (d *singleClusterDownloader) downloadRangeReader(key string, offset, size int64) (l int64, reader io.ReadCloser, err error) {
	for i := 0; i < 3; i++ {
		l, reader, err = d.downloadRangeReaderInner(key, offset, size)
		if err == nil {
			break
		}
	}
	return
}

func (d *singleClusterDownloader) downloadRangeReaderInner(key string, offset, size int64) (int64, io.ReadCloser, error) {
	if d.client == nil {
		return -1, nil, errors.New("obsclient is nil")
	}

	input := &obs.GetObjectInput{}
	input.Bucket = d.bucket
	input.Key = key
	output, err := d.client.GetObject(input, obs.WithCustomHeader("Range", generateRange(offset, size)))

	if err != nil {
		return -1, nil, err
	}

	if output.StatusCode != http.StatusPartialContent {
		output.Body.Close()
		return -1, nil, err
	}

	return output.ContentLength, output.Body, err
}

// DownloadRangeBytes 下载指定对象的指定范围到内存中
func (d *singleClusterDownloader) downloadRangeBytes(key string, offset, size int64) (l int64, data []byte, err error) {
	l, r, err := d.downloadRangeReaderInner(key, offset, size)
	if err != nil {
		return l, nil, err
	}
	b, err := io.ReadAll(r)
	r.Close()
	return l, b, err
}

func (d *singleClusterDownloader) downloadBytes(key string) (data []byte, err error) {
	for i := 0; i < 3; i++ {
		data, err = d.downloadBytesInner(key)
		if err == nil {
			break
		}
	}
	return
}

func (d *singleClusterDownloader) downloadBytesInner(key string) ([]byte, error) {

	if d.client == nil {
		return nil, errors.New("obsclient is nil")
	}

	input := &obs.GetObjectInput{}
	input.Bucket = d.bucket
	input.Key = key

	output, err := d.client.GetObject(input)

	if err != nil {
		return nil, err
	}
	defer output.Body.Close()

	if output.StatusCode != http.StatusOK {
		return nil, errors.New("output.Status")
	}
	return io.ReadAll(output.Body)
}

func (d *singleClusterDownloader) downloadFile(key, path string) (f *os.File, err error) {
	failedIoHosts := make(map[string]struct{})
	for i := 0; i < 3; i++ {
		f, err = d.downloadFileInner(key, path, failedIoHosts)
		if err == nil {
			return
		}
	}
	return
}
func (d *singleClusterDownloader) downloadFileInner(key, path string, failedIoHosts map[string]struct{}) (*os.File, error) {
	var length int64 = 0
	var f *os.File
	var err error
	f, err = os.OpenFile(path, os.O_RDWR, 0644)
	if err == nil {
		length, err = f.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	input := &obs.GetObjectInput{}
	input.Bucket = d.bucket
	input.Key = key
	output, err := d.client.GetObject(input, obs.WithCustomHeader("Range", fmt.Sprintf("bytes=%d-", length)))

	if err != nil {
		return nil, err
	}
	defer output.Body.Close()

	if output.StatusCode != http.StatusOK && output.StatusCode != http.StatusPartialContent {
		return nil, err
	}
	ctLength := output.ContentLength
	if f == nil {
		f, err = os.OpenFile(path, os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
	}

	n, err := io.Copy(f, output.Body)
	if err != nil {
		return nil, err
	}
	if ctLength != n {
		fmt.Println("download length not equal", ctLength, n)
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return f, nil
}
