package redis

import (
	"time"

	"github.com/bsm/redislock"
)

type Lock interface {
	// Release frees the resouce a lock is holding so other locks can hold them when needed
	Release() error
}

type Locker interface {
	// Obtain tries to lock a resource referred by `key` during `ttl` duration.
	// If a lock can't be obtained, it's return should be (nil, err != nil)
	// Consider LockOptions default values when implementing a Locker
	Obtain(key string, ttl time.Duration, opt LockOptions) (Lock, error)
}

// LockOptions define available settings for Locker.Obtain
type LockOptions struct {
	// RetryBackoff is the time to wait between retries
	// Default: 100ms
	RetryBackoff time.Duration
	// RetryCount is the amount of times to retry obtaining the lock
	// in case it's not available on previous tries
	// Default: 0
	RetryCount int
}

func DefaultLockOptions() LockOptions {
	return LockOptions{
		RetryBackoff: 100 * time.Millisecond,
		RetryCount:   0,
	}
}

func (l LockOptions) toRedisLockOptions() redislock.Options {
	return redislock.Options{
		RetryBackoff: l.RetryBackoff,
		RetryCount:   l.RetryCount,
	}
}
