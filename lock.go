package redis

import (
	"time"

	"github.com/bsm/redislock"
)

type Lock interface {
	Release() error
}

type Locker interface {
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

func (l LockOptions) toRedisLockOptions() redislock.Options {
	return redislock.Options{
		RetryBackoff: l.RetryBackoff,
		RetryCount:   l.RetryCount,
	}
}
