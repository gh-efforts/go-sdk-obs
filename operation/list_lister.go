package operation

import "context"

type clusterLister interface {
	listStat(ctx context.Context, keys []string) ([]*FileStat, error)
	listPrefix(ctx context.Context, prefix string) ([]string, error)
	listPrefixToChannel(ctx context.Context, prefix string, output chan<- string) error
	deleteKeys(ctx context.Context, keys []string) ([]*DeleteKeysError, error)
	delete(ctx context.Context, key string) error
}

// Lister 列举器
type Lister struct {
	clusterLister
}

// ListPrefix 根据前缀列举存储空间
func (l *Lister) ListPrefix(prefix string) []string {
	keys, err := l.listPrefix(context.Background(), prefix)
	if err != nil {
		return []string{}
	}
	return keys
}

// ListStat 获取指定对象列表的元信息
func (l *Lister) ListStat(keys []string) []*FileStat {
	fileStats, err := l.listStat(context.Background(), keys)
	if err != nil {
		return []*FileStat{}
	}
	return fileStats
}

// DeleteKeys 删除多个对象
func (l *Lister) DeleteKeys(keys []string) ([]*DeleteKeysError, error) {
	return l.deleteKeys(context.Background(), keys)
}

// Delete 删除指定对象
func (l *Lister) Delete(key string) error {
	return l.delete(context.Background(), key)
}
