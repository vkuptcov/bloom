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

type RedisBloom struct {
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

var UnexpectedHeaderValueErr = errors.New("unexpected header value")

func NewRedisBloom(
	redisClient redisclients.RedisClient,
	cachePrefix string,
	filterParams FilterParams,
) *RedisBloom {
	bitsCount, hashFunctionsNumber := filterParams.EstimatedBucketParameters()
	return &RedisBloom{
		client:              redisClient,
		cachePrefix:         cachePrefix,
		filterParams:        filterParams,
		bitsCount:           bitsCount,
		hashFunctionsNumber: hashFunctionsNumber,
	}
}

func (r *RedisBloom) Init(ctx context.Context) error {
	pipeliner := r.client.Pipeliner(ctx)
	for bucketID := uint64(0); bucketID < uint64(r.filterParams.BucketsCount); bucketID++ {
		key := r.redisKeyByBucket(bucketID)
		r.writeHeader(pipeliner, key)
		r.ensureFilterSize(pipeliner, key)
	}
	return errors.Wrap(pipeliner.Exec(), "Bloom filter buckets init error")
}

func (r *RedisBloom) Add(ctx context.Context, data []byte) error {
	offsets := r.bitsOffset(data)
	key := r.redisKey(data)
	pipeliner := r.client.Pipeliner(ctx)
	pipeliner.SetBits(key, offsets...)
	pipeliner.Publish(r.cachePrefix, data)
	return errors.Wrap(pipeliner.Exec(), "filter update in Redis failed")
}

func (r *RedisBloom) Test(ctx context.Context, data []byte) (bool, error) {
	offsets := r.bitsOffset(data)
	key := r.redisKey(data)
	return r.client.CheckBits(ctx, key, offsets...)
}

func (r *RedisBloom) WriteTo(ctx context.Context, bucketID uint64, stream io.Writer) (int64, error) {
	b, gettingBytesErr := r.client.Get(ctx, r.redisKeyByBucket(bucketID))
	if gettingBytesErr != nil {
		return 0, errors.Wrapf(gettingBytesErr, "getting filter data failed for bucket %d", bucketID)
	}
	size, writeErr := stream.Write(b[0:(len(b) - int(wordSize/8))])
	return int64(size), errors.Wrapf(writeErr, "write filter data failed for bucket %d", bucketID)
}

// MergeWith adds data from buf into the existing data in Redis
func (r *RedisBloom) MergeWith(ctx context.Context, bucketID uint64, addFinalBit bool, buf *bytes.Buffer) error {
	if headerCheckErr := r.checkHeader(buf.Bytes()); headerCheckErr != nil {
		return headerCheckErr
	}
	dstKey := r.redisKeyByBucket(bucketID)
	tmpKey := dstKey + "-tmp"
	pipeliner := r.client.Pipeliner(ctx)
	pipeliner.
		Set(tmpKey, buf.Bytes(), 0).
		BitOpOr(dstKey, tmpKey).
		Del(tmpKey)
	r.writeHeader(pipeliner, dstKey)
	r.ensureFilterSize(pipeliner, dstKey)
	if addFinalBit {
		pipeliner.SetBits(dstKey, r.maxLenWithHeader()+1)
	}
	return pipeliner.Exec()
}

func (r *RedisBloom) writeHeader(pipeliner redisclients.Pipeliner, key string) {
	pipeliner.BitField(
		key,
		// bits number for *bloom.BloomFilter
		"SET", "i64", "#0", int64(r.bitsCount),
		// hash functions count for *bloom.BloomFilter
		"SET", "i64", "#1", int64(r.hashFunctionsNumber),
		// bits number for *bitset.BitSet
		"SET", "i64", "#2", int64(r.bitsCount),
	)
}

func (r *RedisBloom) ensureFilterSize(pipeliner redisclients.Pipeliner, key string) {
	pipeliner.SetBits(key, r.maxLenWithHeader()+wordSize)
}

func (r *RedisBloom) bitsOffset(data []byte) []uint64 {
	locations := bloom.Locations(data, r.hashFunctionsNumber)
	for idx, l := range locations {
		locations[idx] = redisOffset(l % uint64(r.bitsCount))
	}
	return locations
}

func (r *RedisBloom) redisKey(data []byte) string {
	return r.redisKeyByBucket(r.filterParams.BucketID(data))
}

func (r *RedisBloom) maxLenWithHeader() uint64 {
	return dataOffset + ((uint64(r.bitsCount)/wordSize)+1)*wordSize
}

func (r *RedisBloom) redisKeyByBucket(bucketID uint64) string {
	return r.cachePrefix +
		"|" + strconv.FormatUint(uint64(r.bitsCount), 10) +
		"|" + strconv.FormatUint(uint64(r.hashFunctionsNumber), 10) +
		"|" + strconv.FormatUint(bucketID, 10)
}

//nolint:unused // we need it later
func (r *RedisBloom) checkRedisFilterState(ctx context.Context, bucketID uint64) (isInitialized bool, err error) {
	key := r.redisKeyByBucket(bucketID)
	return r.client.CheckBits(ctx, key, r.maxLenWithHeader()+1)
}

func (r *RedisBloom) checkHeader(data []byte) error {
	reader := bytes.NewReader(data)
	var bitsCountForFilter, hashFunctionNumber, bitsCountForSet uint64
	if headReadErr := binary.Read(reader, binary.BigEndian, &bitsCountForFilter); headReadErr != nil {
		return errors.Wrap(headReadErr, "bits count for filter read error")
	}
	if headReadErr := binary.Read(reader, binary.BigEndian, &hashFunctionNumber); headReadErr != nil {
		return errors.Wrap(headReadErr, "hash functions number for filter read error")
	}
	if headReadErr := binary.Read(reader, binary.BigEndian, &bitsCountForSet); headReadErr != nil {
		return errors.Wrap(headReadErr, "bits count for bitset read error")
	}
	if uint64(r.bitsCount) != bitsCountForFilter {
		return errors.Wrapf(
			UnexpectedHeaderValueErr,
			"unexpected bits count for filter. %d given, %d expected", bitsCountForFilter, r.bitsCount,
		)
	}
	if uint64(r.bitsCount) != bitsCountForSet {
		return errors.Wrapf(
			UnexpectedHeaderValueErr,
			"unexpected bits count for set. %d given, %d expected", bitsCountForSet, r.bitsCount,
		)
	}
	if uint64(r.hashFunctionsNumber) != hashFunctionNumber {
		return errors.Wrapf(
			UnexpectedHeaderValueErr,
			"unexpected hash functions number. %d given, %d expected", hashFunctionNumber, r.hashFunctionsNumber,
		)
	}
	return nil
}

func redisOffset(bitNum uint64) uint64 {
	wordNum := bitNum / wordSize
	wordShift := wordSize - (bitNum & (wordSize - 1)) - 1
	return dataOffset + wordNum*wordSize + wordShift
}
