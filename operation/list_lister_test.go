package operation

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getClearedListerForTest(t *testing.T) *Lister {
	return &Lister{getClearedSingleClusterListerForTest(t)}
}

func TestListPrefix(t *testing.T) {
	lister := getClearedListerForTest(t)
	config := getConfig1()

	uploader := NewUploader(config)

	result := lister.ListPrefix("")
	_, err := lister.DeleteKeys(result)
	assert.NoError(t, err)

	err = uploader.UploadData([]byte("test1"), "test1")
	assert.NoError(t, err)

	err = uploader.UploadData([]byte("test2"), "test2")
	assert.NoError(t, err)

	result = lister.ListPrefix("")
	assert.Contains(t, result, "test1")
	assert.Contains(t, result, "test2")
}

func TestLister_ListStat(t *testing.T) {
	lister := getClearedListerForTest(t)
	config := getConfig1()

	uploader := NewUploader(config)

	type TestCase struct {
		name    string
		content []byte
	}
	testCases := []TestCase{
		{name: "test1", content: []byte{1, 2, 3}},
		{name: "test2", content: []byte("test123")},
		{name: "test3", content: []byte("123")},
	}

	for _, tc := range testCases {
		err := uploader.UploadData(tc.content, tc.name)
		assert.NoError(t, err)
	}

	// 提取keys
	keys := make([]string, len(testCases))
	for i, _ := range testCases {
		keys[i] = testCases[i].name
	}

	defer lister.DeleteKeys(keys)

	// 列举出所有文件的stat
	fileStats := lister.ListStat(keys)

	for i, stat := range fileStats {
		assert.Equal(t, testCases[i].name, stat.Name)
		assert.Equal(t, int64(len(testCases[i].content)), stat.Size)
	}
}

func TestLister_Delete(t *testing.T) {
	lister := getClearedListerForTest(t)
	config := getConfig1()

	uploader := NewUploader(config)

	// 创建文件 test1
	err := uploader.UploadData([]byte("test1"), "test1")
	defer lister.Delete("test1")
	assert.NoError(t, err)

	// 列举出所有文件
	result := lister.ListPrefix("")
	assert.NotEmpty(t, result)

	// 测试文件 test1 应当存在
	assert.Contains(t, result, "test1")

	// 删除文件 test1
	err = lister.Delete("test1")
	assert.NoError(t, err)

	// 列举出所有文件
	result = lister.ListPrefix("")

	// 测试文件 test1 应当不存在
	assert.NotContains(t, result, "test1")
}

func TestLister_DeleteKeys(t *testing.T) {
	lister := getClearedListerForTest(t)
	config := getConfig1()

	uploader := NewUploader(config)

	type TestCase struct {
		name    string
		content []byte
	}
	testCases := []TestCase{
		{name: "test1", content: []byte{1, 2, 3}},
		{name: "test2", content: []byte("test123")},
		{name: "test3", content: []byte("123")},
	}

	for _, tc := range testCases {
		err := uploader.UploadData(tc.content, tc.name)
		assert.NoError(t, err)
	}

	result := lister.ListPrefix("")

	// 提取keys，并验证每个key是否存在于result中
	keys := make([]string, len(testCases))
	for i, tc := range testCases {
		keys[i] = tc.name
		assert.Contains(t, result, tc.name)
	}

	// 批量删除
	lister.DeleteKeys(keys)

	// 删除结束后每个key都不存在result中了
	result = lister.ListPrefix("")

	for _, key := range keys {
		assert.NotContains(t, result, key)
	}
}

func makeLotsFilesWithPrefix(t *testing.T, files uint, batchConcurrency int, prefix string) (paths []string) {
	config := getConfig1()
	uploader := NewUploader(config)

	pool := NewGoroutinePool(batchConcurrency)
	for i := uint(0); i < files; i++ {
		func(id uint) {
			p := fmt.Sprintf("%stest%d", prefix, id)
			pool.Go(func(ctx context.Context) (err error) {
				return uploader.UploadData(nil, p)
			})
		}(i)
	}
	err := pool.Wait(context.Background())
	assert.NoError(t, err)

	// 文件列表
	for i := uint(0); i < files; i++ {
		paths = append(paths, fmt.Sprintf("test%d", i))
	}
	return paths
}

func makeLotsFiles(t *testing.T, files uint, batchConcurrency int) (paths []string) {
	return makeLotsFilesWithPrefix(t, files, batchConcurrency, "")
}

func TestDeleteLotsFile(t *testing.T) {
	lister := getClearedListerForTest(t)
	makeLotsFiles(t, 2000, 500)

	paths := lister.ListPrefix("")
	assert.Equal(t, 2000, len(paths))
	_, err := lister.DeleteKeys(paths)
	assert.NoError(t, err)
	assert.Empty(t, lister.ListPrefix(""))
}

func TestListStatLotsFile(t *testing.T) {
	lister := getClearedListerForTest(t)
	makeLotsFiles(t, 2000, 500)

	paths := makeLotsFiles(t, 2000, 500)
	defer lister.DeleteKeys(paths)
	stats := lister.ListStat(paths)
	assert.Equal(t, 2000, len(stats))
}
