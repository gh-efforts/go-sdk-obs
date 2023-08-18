package operation

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	obs "github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

type singleClusterUploader struct {
	bucket        string
	partSize      int64
	upConcurrency int
	client        *obs.ObsClient
}

func newSingleClusterUploader(c *Config) *singleClusterUploader {
	obsClient, err := obs.New(c.Ak, c.Sk, c.EndPoint)

	if err != nil {
		fmt.Printf("Create obsClient error, errMsg: %s\n", err.Error())
	}

	partSize := c.PartSize * 1024 * 1024
	if partSize < 4*1024*1024 {
		partSize = 4 * 1024 * 1024
	}
	return &singleClusterUploader{
		bucket:        c.Bucket,
		partSize:      partSize,
		client:        obsClient,
		upConcurrency: c.UpConcurrency,
	}
}

func (p *singleClusterUploader) uploadData(data []byte, key string) error {
	if p.client == nil {
		return errors.New("obsclient is nil")
	}

	t := time.Now()
	defer func() {
		fmt.Printf("上传对象总共花了%d\n", time.Since(t))
	}()
	key = strings.TrimPrefix(key, "/")
	input := &obs.PutObjectInput{}
	input.Bucket = p.bucket
	input.Key = key
	input.Body = bytes.NewReader(data)
	_, err := p.client.PutObject(input)

	return err
}

func (p *singleClusterUploader) upload(file string, key string) (err error) {
	if p.client == nil {
		return errors.New("obsclient is nil")
	}

	t := time.Now()
	defer func() {
		fmt.Printf("上传对象总共花了%d\n", time.Since(t))
	}()
	key = strings.TrimPrefix(key, "/")

	f, err := os.Open(file)
	if err != nil {
		fmt.Printf("open file failed: %d\n", err)
		return err
	}
	defer f.Close()

	fInfo, err := f.Stat()
	if err != nil {
		fmt.Printf("get file stat failed: %v\n", err)
		return err
	}

	if fInfo.Size() <= 50*1024*1024 {
		// 小对象
		input := &obs.PutFileInput{}
		input.Bucket = p.bucket
		input.Key = key
		input.SourceFile = file
		_, err = p.client.PutFile(input)
		return
	}

	input := &obs.UploadFileInput{}
	input.Bucket = p.bucket
	input.Key = key
	input.UploadFile = file
	input.EnableCheckpoint = true
	input.PartSize = p.partSize
	input.TaskNum = p.upConcurrency
	_, err = p.client.UploadFile(input)
	return
}
