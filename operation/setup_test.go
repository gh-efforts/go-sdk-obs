package operation

import (
	"log"
	"os"
	"testing"
)

func getAccessKey() string {
	return os.Getenv("QINIU_ACCESS_KEY")
}

func getSecretKey() string {
	return os.Getenv("QINIU_SECRET_KEY")
}

func getEndPoint() string {
	return os.Getenv("QINIU_TEST_ENDPOINT")
}

func getBucket() string {
	return os.Getenv("QINIU_TEST_BUCKET")
}

func getConfig1() *Config {
	config := &Config{
		Ak:       getAccessKey(),
		Sk:       getSecretKey(),
		EndPoint: getEndPoint(),
		Bucket:   getBucket(),
	}
	return config
}

// 检查是否应该跳过测试
func checkSkipTest(t *testing.T) {
	if os.Getenv("QINIU_KODO_TEST") == "" {
		t.Skip("skipping test in short mode.")
	}
}

func TestMain(m *testing.M) {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	os.Exit(m.Run())
}
