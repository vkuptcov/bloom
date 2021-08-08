package bloom

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"strconv"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/pkg/errors"
	"github.com/vkuptcov/bloom/redisclients"
)

type redisBloom struct {
	client      redisclients.RedisClient
	cachePrefix string

	filterParams FilterParams

	bitsCount           uint
	hashFunctionsNumber uint
}

// the wordSize of a bit set
const wordSize = uint64(64)

// first 3 words are reserved for BloomFilter header that is compatible
// with internal implementations of the in-memory filer
const dataOffset = wordSize * 3

var UnexpectedHeaderValue = errors.New("Unexpected header value")

func NewRedisBloom(
	redisClient redisclients.RedisClient,
	cachePrefix string,
	filterParams FilterParams,
) *redisBloom {
	bitsCount, hashFunctionsNumber := filterParams.EstimatedBucketParameters()
	return &redisBloom{
		client:              redisClient,
		cachePrefix:         cachePrefix,
		filterParams:        filterParams,
		bitsCount:           bitsCount,
		hashFunctionsNumber: hashFunctionsNumber,
	}
}

func (r *redisBloom) Init(ctx context.Context) error {
	pipeliner := r.client.Pipeliner(ctx)
	for bucketID := uint64(0); bucketID < uint64(r.filterParams.BucketsCount); bucketID++ {
		headerExists, checkHeaderErr := r.checkHeader(ctx, bucketID)
		if checkHeaderErr != nil {
			return checkHeaderErr
		}
		if headerExists {
			continue
		}
		key := r.redisKeyByBucket(bucketID)
		pipeliner.BitField(
			key,
			// bits number for *bloom.BloomFilter
			"SET", "i64", "#0", int64(r.bitsCount),
			// hash functions count for *bloom.BloomFilter
			"SET", "i64", "#1", int64(r.hashFunctionsNumber),
			// bits number for *bitset.BitSet
			"SET", "i64", "#2", int64(r.bitsCount),
		)
		pipeliner.SetBits(key, dataOffset+((uint64(r.bitsCount)/wordSize)+1)*wordSize+1)
	}
	return errors.Wrap(pipeliner.Exec(), "Bloom filter buckets init error")
}

func (r *redisBloom) Add(ctx context.Context, data []byte) error {
	offsets := r.bitsOffset(data)
	key := r.redisKey(data)
	pipeliner := r.client.Pipeliner(ctx)
	pipeliner.SetBits(key, offsets...)
	pipeliner.Publish(r.cachePrefix, data)
	return errors.Wrap(pipeliner.Exec(), "filter update in Redis failed")
}

func (r *redisBloom) Test(ctx context.Context, data []byte) (bool, error) {
	offsets := r.bitsOffset(data)
	key := r.redisKey(data)
	return r.client.CheckBits(ctx, key, offsets...)
}

func (r *redisBloom) WriteTo(ctx context.Context, bucketID uint64, stream io.Writer) (int64, error) {
	b, gettingBytesErr := r.client.Get(ctx, r.redisKeyByBucket(bucketID))
	if gettingBytesErr != nil {
		return 0, errors.Wrapf(gettingBytesErr, "getting filter data failed for bucket %d", bucketID)
	}
	size, writeErr := stream.Write(b[0:(len(b) - 1)])
	return int64(size), errors.Wrapf(writeErr, "write filter data failed for bucket %d", bucketID)
}

func (r *redisBloom) bitsOffset(data []byte) []uint64 {
	locations := bloom.Locations(data, r.hashFunctionsNumber)
	for idx, l := range locations {
		locations[idx] = redisOffset(l % uint64(r.bitsCount))
	}
	return locations
}

func (r *redisBloom) redisKey(data []byte) string {
	return r.redisKeyByBucket(r.filterParams.BucketID(data))
}

func (r *redisBloom) redisKeyByBucket(bucketID uint64) string {
	return r.cachePrefix +
		"|" + strconv.FormatUint(uint64(r.bitsCount), 10) +
		"|" + strconv.FormatUint(uint64(r.hashFunctionsNumber), 10) +
		"|" + strconv.FormatUint(bucketID, 10)
}

func (r *redisBloom) checkHeader(ctx context.Context, bucketID uint64) (headerExists bool, err error) {
	headerBytes, headerGetErr := r.client.GetRange(ctx, r.redisKeyByBucket(bucketID), 0, int64(wordSize/8*3))
	if headerGetErr != nil {
		return false, errors.Wrapf(headerGetErr, "check header failed for %d", bucketID)
	}
	if len(headerBytes) == 0 {
		return false, nil
	}
	reader := bytes.NewReader(headerBytes)
	var bitsCountForFilter, hashFunctionNumber, bitsCountForSet uint64
	if headReadErr := binary.Read(reader, binary.BigEndian, &bitsCountForFilter); headReadErr != nil {
		return true, errors.Wrapf(headReadErr, "bits count for filter read error for %d", bucketID)
	}
	if headReadErr := binary.Read(reader, binary.BigEndian, &hashFunctionNumber); headReadErr != nil {
		return true, errors.Wrapf(headReadErr, "hash functions number for filter read error for %d", bucketID)
	}
	if headReadErr := binary.Read(reader, binary.BigEndian, &bitsCountForSet); headReadErr != nil {
		return true, errors.Wrapf(headReadErr, "bits count for bitset read error for %d", bucketID)
	}
	if uint64(r.bitsCount) != bitsCountForFilter {
		return true, errors.Wrapf(
			UnexpectedHeaderValue,
			"unexpected bits count for filter. %d given, %d expected", bitsCountForFilter, r.bitsCount,
		)
	}
	if uint64(r.bitsCount) != bitsCountForSet {
		return true, errors.Wrapf(
			UnexpectedHeaderValue,
			"unexpected bits count for set. %d given, %d expected", bitsCountForSet, r.bitsCount,
		)
	}
	if uint64(r.hashFunctionsNumber) != hashFunctionNumber {
		return true, errors.Wrapf(
			UnexpectedHeaderValue,
			"unexpected hash functions number. %d given, %d expected", hashFunctionNumber, r.hashFunctionsNumber,
		)
	}
	return true, nil
}

func redisOffset(bitNum uint64) uint64 {
	wordNum := bitNum / wordSize
	wordShift := wordSize - (bitNum & (wordSize - 1)) - 1
	return dataOffset + wordNum*wordSize + wordShift
}
