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
	// ExponentialBackoff MinTime retry
	// Default: 16ms
	// the recommended minimum value is not less than 16ms (redislock doc)
	MinTime time.Duration
	// ExponentialBackoff MinTime retry
	// Default: 64ms
	MaxTime time.Duration
}

func DefaultLockOptions() LockOptions {
	return LockOptions{
		MinTime: 16 * time.Millisecond,
		MaxTime: 64 * time.Millisecond,
	}
}

func (l LockOptions) toRedisLockOptions() redislock.Options {
	return redislock.Options{
		RetryStrategy: redislock.ExponentialBackoff(l.MinTime, l.MaxTime),
	}
}
