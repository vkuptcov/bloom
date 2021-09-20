package bloom

import (
	"sync"
)

type initializedBuckets struct {
	buckets map[int]struct{}
	rwm     sync.RWMutex
}

//nolint:unused // keep it for next iterations
func (b *initializedBuckets) isInitialized(bucketID int) bool {
	b.rwm.RLock()
	defer b.rwm.RUnlock()
	_, exists := b.buckets[bucketID]
	return exists
}

func (b *initializedBuckets) initialize(bucketID int) {
	b.rwm.Lock()
	defer b.rwm.Unlock()
	if b.buckets == nil {
		b.buckets = map[int]struct{}{}
	}
	b.buckets[bucketID] = struct{}{}
}

//nolint:unused // keep it for next iterations
func (b *initializedBuckets) deinitialize(bucketID int) {
	b.rwm.Lock()
	defer b.rwm.Unlock()
	delete(b.buckets, bucketID)
}

func (b *initializedBuckets) len() int {
	b.rwm.RLock()
	defer b.rwm.RUnlock()
	return len(b.buckets)
}
