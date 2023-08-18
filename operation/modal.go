package operation

import (
	"time"
)

// FileStat 文件元信息
type FileStat struct {
	Name string
	Size int64
	code int
}

// Config 配置文件
type Config struct {
	Ak               string
	Sk               string
	EndPoint         string
	Bucket           string
	PartSize         int64
	UpConcurrency    int
	BatchConcurrency int
	BatchSize        int
}

type ListItem struct {
	Key   string
	Hash  string
	Fsize int64
	// 注：这里跟七牛不一样
	PutTime  time.Time
	MimeType string
	EndUser  string
}

type DeleteKeysError SingleKeyError

// 注：这里跟七牛不一样
type SingleKeyError struct {
	Message string
	Code    string
	Name    string
}

type Entry struct {
	Hash     string
	Fsize    int64
	PutTime  int64
	MimeType string
	EndUser  string
}

type BatchStatItemRet struct {
	Data  Entry
	Error string
	Code  int
}
