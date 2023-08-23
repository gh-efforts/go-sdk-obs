package operation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	obs "github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

type singleClusterLister struct {
	bucket           string
	batchConcurrency int
	batchSize        int
	client           *obs.ObsClient
}

func newSingleClusterLister(c *Config) *singleClusterLister {
	obsClient, err := obs.New(c.Ak, c.Sk, c.EndPoint)

	if err != nil {
		fmt.Printf("Create obsClient error, errMsg: %s\n", err.Error())
	}

	lister := singleClusterLister{
		bucket:           c.Bucket,
		client:           obsClient,
		batchSize:        c.BatchSize,
		batchConcurrency: c.BatchConcurrency,
	}

	if lister.batchConcurrency <= 0 {
		lister.batchConcurrency = 20
	}
	if lister.batchSize <= 0 {
		lister.batchSize = 100
	}

	return &lister

}

// 列举指定前缀的文件到channel中
func (l *singleClusterLister) listPrefixToChannel(ctx context.Context, prefix string, ch chan<- string) error {
	marker := ""
	for {
		res, markerOut, err := func() (res []ListItem, markerOut string, err error) {
			res, _, markerOut, err = l.list(ctx, prefix, "", marker, 1000)
			if err != nil && err != io.EOF {
				return nil, "", err
			}
			return res, markerOut, nil
		}()

		if err != nil {
			return err
		}

		for _, item := range res {
			ch <- item.Key
		}

		if markerOut == "" {
			break
		}
		marker = markerOut
	}
	return nil
}

// 列举指定前缀的所有文件
func (l *singleClusterLister) listPrefix(ctx context.Context, prefix string) (files []string, err error) {
	ch := make(chan string, 1000)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for c := range ch {
			files = append(files, c)
		}
	}()

	err = l.listPrefixToChannel(ctx, prefix, ch)
	close(ch)
	wg.Wait()

	if err != nil {
		return nil, err
	}
	return files, nil
}

func (l *singleClusterLister) list(ctx context.Context, prefix, delimiter, marker string, limit int) (entries []ListItem, commonPrefixes []string, markerOut string, err error) {

	if l.client == nil {
		return nil, nil, "", errors.New("obsclient is nil")
	}

	input := &obs.ListObjectsInput{}
	input.Bucket = l.bucket
	input.Prefix = prefix
	input.Delimiter = delimiter
	input.Marker = marker
	input.MaxKeys = limit
	input.EncodingType = "url"
	output, err := l.client.ListObjects(input)

	if err != nil {
		return
	}

	if output.NextMarker == "" {
		return convertListItem(output.Contents), output.CommonPrefixes, "", io.EOF
	}

	return convertListItem(output.Contents), output.CommonPrefixes, output.NextMarker, nil
}

func (l *singleClusterLister) listStat(ctx context.Context, paths []string) ([]*FileStat, error) {

	if l.client == nil {
		return nil, errors.New("obsclient is nil")
	}

	// 并发数计算
	concurrency := (len(paths) + l.batchSize - 1) / l.batchSize
	if concurrency > l.batchConcurrency {
		concurrency = l.batchConcurrency
	}
	var (
		stats = make([]*FileStat, len(paths))
		pool  = NewGoroutinePool(concurrency)
	)
	// 分批处理
	for i := 0; i < len(paths); i += l.batchSize {
		// 计算本次批量处理的数量
		size := l.batchSize
		if size > len(paths)-i {
			size = len(paths) - i
		}

		// paths 是这批要删除的文件
		// index 是这批文件的起始位置
		func(paths []string, index int) {
			pool.Go(func(ctx context.Context) error {
				func() {
					for j, key := range paths {
						input := &obs.GetObjectMetadataInput{}
						input.Bucket = l.bucket
						input.Key = key
						output, _ := l.client.GetObjectMetadata(input)
						if output.StatusCode <= 300 {
							stats[index+j] = &FileStat{
								Name: key,
								Size: output.ContentLength,
								code: output.StatusCode,
							}
						} else {
							stats[index+j] = &FileStat{
								Name: paths[j],
								Size: -1,
								code: output.StatusCode,
							}
						}
					}
				}()
				return nil
			})
		}(paths[i:i+size], i)
	}

	// 等待所有的批量任务完成，如果出错，直接结束返回错误
	if err := pool.Wait(ctx); err != nil {
		return nil, err
	}

	return stats, nil
}

func (l *singleClusterLister) deleteKeys(ctx context.Context, paths []string) ([]*DeleteKeysError, error) {

	if l.client == nil {
		return nil, errors.New("obsclient is nil")
	}

	// 并发数计算
	concurrency := (len(paths) + l.batchSize - 1) / l.batchSize
	if concurrency > l.batchConcurrency {
		concurrency = l.batchConcurrency
	}
	var (
		errors = make([]*DeleteKeysError, len(paths))
		pool   = NewGoroutinePool(concurrency)
	)
	// 分批处理
	for i := 0; i < len(paths); i += l.batchSize {
		// 计算本次批量处理的数量
		size := l.batchSize
		if size > len(paths)-i {
			size = len(paths) - i
		}

		// paths 是这批要删除的文件
		// index 是这批文件的起始位置
		func(paths []string, index int) {
			pool.Go(func(ctx context.Context) error {
				res, _ := func() ([]obs.Error, error) {
					input := &obs.DeleteObjectsInput{}
					input.Bucket = l.bucket
					objects := make([]obs.ObjectToDelete, len(paths))
					for index, obj := range paths {
						objects[index] = obs.ObjectToDelete{Key: obj}
					}
					input.Objects = objects[:]
					output, err := l.client.DeleteObjects(input)

					if err == nil {
						return output.Errors, nil
					}
					return nil, err
				}()
				for j, v := range res {
					errors[index+j] = &DeleteKeysError{
						Name:    v.Key,
						Message: v.Message,
						Code:    v.Code,
					}
				}
				return nil
			})

		}(paths[i:i+size], i)
	}

	// 等待所有的批量删除任务完成，如果出错了，直接结束返回错误
	if err := pool.Wait(ctx); err != nil {
		return nil, err
	}

	return errors, nil
}

func (l *singleClusterLister) delete(ctx context.Context, key string) (err error) {

	if l.client == nil {
		return errors.New("obsclient is nil")
	}

	input := &obs.DeleteObjectInput{}
	input.Bucket = l.bucket
	input.Key = key
	_, err = l.client.DeleteObject(input)
	return err
}

func (l *singleClusterLister) stat(ctx context.Context, key string) (*Entry, error) {

	if l.client == nil {
		return nil, errors.New("obsclient is nil")
	}

	input := &obs.GetObjectMetadataInput{}
	input.Bucket = l.bucket
	input.Key = key
	output, err := l.client.GetObjectMetadata(input)

	if err != nil {
		return nil, err
	}
	return &Entry{
		Hash:     output.ETag,
		Fsize:    output.ContentLength,
		PutTime:  output.LastModified,
		MimeType: output.ContentType,
		EndUser:  "",
	}, nil
}

func (l *singleClusterLister) statBucket(ctx context.Context) (*obs.GetBucketMetadataOutput, error) {
	if l.client == nil {
		return nil, errors.New("obsclient is nil")
	}

	return l.client.GetBucketMetadata(&obs.GetBucketMetadataInput{Bucket: l.bucket})
}
