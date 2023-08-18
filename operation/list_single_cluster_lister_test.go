package operation

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getClearedSingleClusterListerForTest(t *testing.T) *singleClusterLister {
	checkSkipTest(t)
	l := newSingleClusterLister(getConfig1())
	clearBucket(t, l)
	return l
}

func clearBucket(t *testing.T, l clusterLister) {
	// 列举所有
	ch := make(chan string, 1000)
	go func() {
		defer close(ch)
		err := l.listPrefixToChannel(context.Background(), "", ch)
		assert.NoError(t, err)
	}()

	errCh := make(chan DeleteKeysError, 1)
	go func() {
		for range errCh {
			// ignore error
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// TODO
			// err := l.deleteKeysFromChannel(ch, true, errCh)
			// assert.NoError(t, err)
		}()
	}
	wg.Wait()
}

func TestSingleClusterLister_upload_listPrefixToChannel_delete(t *testing.T) {

	uploader := NewUploader(getConfig1())

	l := getClearedSingleClusterListerForTest(t)

	ch := make(chan string, 10)
	for i := 0; i < 10; i++ {
		err := uploader.UploadData(nil, fmt.Sprintf("listPrefixToChannel%d", i))
		assert.NoError(t, err)
	}

	go func() {
		defer close(ch)
		err := l.listPrefixToChannel(context.Background(), "listPrefixToChannel", ch)
		assert.NoError(t, err)
	}()

	for i := 0; i < 10; i++ {
		key := <-ch
		assert.Equal(t, fmt.Sprintf("listPrefixToChannel%d", i), key)
		_, err := l.deleteKeys(context.Background(), []string{key})
		assert.NoError(t, err)
	}
}
