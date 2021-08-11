//nolint:unused // keep it for next iterations
package bloom

import (
	"sync"
)

type initializedBuckets struct {
	buckets map[uint32]struct{}
	rwm     sync.RWMutex
}

func (b *initializedBuckets) isInitialized(bucketID uint32) bool {
	b.rwm.RLock()
	defer b.rwm.RUnlock()
	_, exists := b.buckets[bucketID]
	return exists
}

func (b *initializedBuckets) initialize(bucketID uint32) {
	b.rwm.Lock()
	defer b.rwm.Unlock()
	if b.buckets == nil {
		b.buckets = map[uint32]struct{}{}
	}
	b.buckets[bucketID] = struct{}{}
}

func (b *initializedBuckets) deinitialize(bucketID uint32) {
	b.rwm.Lock()
	defer b.rwm.Unlock()
	delete(b.buckets, bucketID)
}

func (b *initializedBuckets) len() int {
	b.rwm.RLock()
	defer b.rwm.RUnlock()
	return len(b.buckets)
}
