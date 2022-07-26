package cache

import (
	"context"
	"sync"
	"time"
)

type GetMissingBlocks func(ctx context.Context) ([]int64, error)

type BlockCache struct {
	ctx              context.Context
	mutex            sync.RWMutex
	updateMutex      sync.RWMutex
	getMissingBlocks GetMissingBlocks
	missingBlocks    []int64
	lastUpdate       time.Time
}

func NewBlockCache(ctx context.Context, getMissingBlocks GetMissingBlocks) *BlockCache {
	blockCache := &BlockCache{
		ctx:              ctx,
		getMissingBlocks: getMissingBlocks,
		missingBlocks:    []int64{},
	}

	return blockCache
}

func (cache *BlockCache) GetMissingBlocks() []int64 {
	cache.mutex.RLock()
	defer cache.mutex.RUnlock()

	return cache.missingBlocks[:]
}

func (cache *BlockCache) UpdateMissingBlocks(ctx context.Context) (bool, error) {
	cache.updateMutex.RLock()
	minutesSinceLastUpdate := time.Since(cache.lastUpdate).Minutes()
	cache.updateMutex.RUnlock()

	if minutesSinceLastUpdate < 1 {
		return false, nil
	}

	cache.updateMutex.Lock()
	defer cache.updateMutex.Unlock()

	minutesSinceLastUpdate = time.Since(cache.lastUpdate).Minutes()

	if minutesSinceLastUpdate < 1 {
		return false, nil
	}

	if ctx == nil || ctx == context.TODO() {
		ctx = cache.ctx
	}

	missingBlocks, err := cache.getMissingBlocks(ctx)
	if err != nil {
		return false, err
	}

	cache.mutex.Lock()
	cache.missingBlocks = missingBlocks
	cache.lastUpdate = time.Now()
	cache.mutex.Unlock()

	return true, nil
}
