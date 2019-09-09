package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/bsm/redislock"
	goredis "github.com/go-redis/redis"
	"github.com/opentracing/opentracing-go"
)

// Client is a minimal set of functions a redis client must implement
type Client interface {
	BLPop(timeout time.Duration, keys ...string) *goredis.StringSliceCmd
	Close() error
	Context() context.Context
	Del(keys ...string) *goredis.IntCmd
	Eval(script string, keys []string, args ...interface{}) *goredis.Cmd
	EvalSha(sha1 string, keys []string, args ...interface{}) *goredis.Cmd
	Exists(keys ...string) *goredis.IntCmd
	Get(key string) *goredis.StringCmd
	HDel(key string, fields ...string) *goredis.IntCmd
	HGet(key, field string) *goredis.StringCmd
	HGetAll(string) *goredis.StringStringMapCmd
	HMGet(string, ...string) *goredis.SliceCmd
	HMSet(string, map[string]interface{}) *goredis.StatusCmd
	HSet(key, field string, value interface{}) *goredis.BoolCmd
	LPop(key string) *goredis.StringCmd
	LRange(key string, start, stop int64) *goredis.StringSliceCmd
	MGet(keys ...string) *goredis.SliceCmd
	MSet(pairs ...interface{}) *goredis.StatusCmd
	Options() *goredis.Options
	Ping() *goredis.StatusCmd
	RPopLPush(source string, destination string) *goredis.StringCmd
	RPush(key string, values ...interface{}) *goredis.IntCmd
	SAdd(key string, members ...interface{}) *goredis.IntCmd
	SCard(key string) *goredis.IntCmd
	SIsMember(key string, member interface{}) *goredis.BoolCmd
	SMembers(key string) *goredis.StringSliceCmd
	SPopN(key string, count int64) *goredis.StringSliceCmd
	SRem(key string, members ...interface{}) *goredis.IntCmd
	ScriptExists(scripts ...string) *goredis.BoolSliceCmd
	ScriptLoad(script string) *goredis.StringCmd
	Set(key string, value interface{}, expiration time.Duration) *goredis.StatusCmd
	SetNX(key string, value interface{}, expiration time.Duration) *goredis.BoolCmd
	TTL(key string) *goredis.DurationCmd
	TxPipeline() goredis.Pipeliner
	WithContext(context.Context) Client
	ZAdd(key string, members ...goredis.Z) *goredis.IntCmd
	ZCard(key string) *goredis.IntCmd
	ZRangeByScore(key string, opt goredis.ZRangeBy) *goredis.StringSliceCmd
	ZRangeByScoreWithScores(key string, opt goredis.ZRangeBy) *goredis.ZSliceCmd
	ZRangeWithScores(key string, start, stop int64) *goredis.ZSliceCmd
	ZRank(key, member string) *goredis.IntCmd
	ZRem(key string, members ...interface{}) *goredis.IntCmd
	ZRevRangeByScore(key string, opt goredis.ZRangeBy) *goredis.StringSliceCmd
	ZRevRangeByScoreWithScores(key string, opt goredis.ZRangeBy) *goredis.ZSliceCmd
	ZRevRangeWithScores(key string, start, stop int64) *goredis.ZSliceCmd
	ZRevRank(key, member string) *goredis.IntCmd
	ZScore(key, member string) *goredis.FloatCmd
}

// LockerClient is a redis Client that has support for Locker operations
// in order to use distributed locking, for example
type LockerClient interface {
	Client
	Locker
}

// BaseClient implements Client since it wraps a *goredis.Client
// and Locker for distributed locking
// it's WithContext calls WithContext in the underlying *goredis.Client
// and instruments opentracing support through go-redis middlewares
type BaseClient struct {
	*goredis.Client
	locker *redislock.Client
	ctx    context.Context
}

// NewClient creates a BaseClient instance with an underlying *goredis.Client
// and a *redislock.Client
func NewClient(opt *goredis.Options) (*BaseClient, error) {
	conn, err := newClientConn(opt)
	if err != nil {
		return nil, err
	}
	locker := redislock.New(conn)
	return &BaseClient{Client: conn, locker: locker}, nil
}

func newClientConn(opt *goredis.Options) (*goredis.Client, error) {
	client := goredis.NewClient(opt)
	if err := waitConnection(client); err != nil {
		return nil, err
	}
	return client, nil
}

// WithContext returns a new *BaseClient with *goredis.Client and *redislock.Client using ctx
func (c *BaseClient) WithContext(ctx context.Context) Client {
	conncpy := c.Client.WithContext(ctx)
	ccpy := &BaseClient{Client: conncpy, locker: c.locker, ctx: ctx}
	instrument(ccpy)
	return ccpy
}

// Obtain tries to hold a lock over `key` during `ttl` duration. It also
func (c BaseClient) Obtain(key string, ttl time.Duration, opt LockOptions) (Lock, error) {
	tags := opentracing.Tags{
		"db.instance": c.Client.Options().DB,
		"db.type":     "redis",
		"span.kind":   "client",
	}
	var lock Lock
	err := trace(c.ctx, "redis obtain lock", tags, func() error {
		var err error
		lock, err = c.obtain(key, ttl, opt)
		return err
	})
	return lock, err
}

func (c BaseClient) obtain(key string, ttl time.Duration, opt LockOptions) (Lock, error) {
	rlopt := opt.toRedisLockOptions()
	return c.locker.Obtain(key, ttl, &rlopt)
}

func waitConnection(client *goredis.Client) error {
	timeout := time.Now().Add(client.Options().DialTimeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		if connected(client) {
			return nil
		}
		if time.Now().After(timeout) {
			return fmt.Errorf("timed out while waiting for Redis to connect")
		}
	}
	return nil
}

func connected(client *goredis.Client) bool {
	result := client.Ping()
	if result == nil {
		return false
	}
	str, err := result.Result()
	if err != nil {
		return false
	}
	return str == "PONG"
}

var _ Client = (*BaseClient)(nil)
var _ LockerClient = (*BaseClient)(nil)
