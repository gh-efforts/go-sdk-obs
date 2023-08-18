package operation

type clusterUploader interface {
	upload(file string, key string) error
	uploadData(data []byte, key string) error
}

// Uploader 上传器
type Uploader struct {
	clusterUploader
}

// NewUploader 根据配置创建上传器
func NewUploader(c *Config) *Uploader {
	return &Uploader{newSingleClusterUploader(c)}
}

// UploadData 上传内存数据到指定对象中
func (p *Uploader) UploadData(data []byte, key string) (err error) {
	return p.uploadData(data, key)
}

// Upload 上传指定文件到指定对象中
func (p *Uploader) Upload(file string, key string) (err error) {
	return p.upload(file, key)
}
