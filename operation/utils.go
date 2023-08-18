package operation

import (
	"fmt"

	obs "github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

func convertListItem(content []obs.Content) []ListItem {
	entries := make([]ListItem, len(content))
	for index, content := range content {
		entries[index] = ListItem{
			Key:     content.Key,
			Hash:    content.ETag,
			Fsize:   content.Size,
			PutTime: content.LastModified,
			// OBS 在列举对象时不会返回 Content-Type，所以这里无法获取
			MimeType: "",
			EndUser:  content.Owner.ID,
		}
	}
	return entries
}

func generateRange(offset, size int64) string {
	if offset == -1 {
		return fmt.Sprintf("bytes=-%d", size)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)
}

func checkObsClient(clent obs.ObsClient) {

}
