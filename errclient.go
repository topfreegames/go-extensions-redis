package redis

import (
	"context"
	"time"

	goredis "github.com/go-redis/redis"
)

var _ Client = &ErrClient{}

// ErrClient returns an error for each Client operation
type ErrClient struct {
	err error
}

func NewErrClient(err error) *ErrClient {
	return &ErrClient{err: err}
}

func (e ErrClient) BLPop(timeout time.Duration, keys ...string) *goredis.StringSliceCmd {
	return goredis.NewStringSliceResult(nil, e.err)
}

func (e ErrClient) Close() error {
	return e.err
}

func (e ErrClient) Context() context.Context {
	return nil
}

func (e ErrClient) Del(keys ...string) *goredis.IntCmd {
	return goredis.NewIntResult(0, e.err)
}

func (e ErrClient) Eval(script string, keys []string, args ...interface{}) *goredis.Cmd {
	return goredis.NewCmdResult(nil, e.err)
}

func (e ErrClient) EvalSha(sha1 string, keys []string, args ...interface{}) *goredis.Cmd {
	return goredis.NewCmdResult(nil, e.err)
}

func (e ErrClient) Exists(keys ...string) *goredis.IntCmd {
	return goredis.NewIntResult(0, e.err)
}

func (e ErrClient) Get(key string) *goredis.StringCmd {
	return goredis.NewStringResult("", e.err)
}

func (e ErrClient) HDel(key string, fields ...string) *goredis.IntCmd {
	return goredis.NewIntResult(0, e.err)
}

func (e ErrClient) HGet(key, field string) *goredis.StringCmd {
	return goredis.NewStringResult("", e.err)
}

func (e ErrClient) HGetAll(string) *goredis.StringStringMapCmd {
	return goredis.NewStringStringMapResult(nil, e.err)
}

func (e ErrClient) HMGet(string, ...string) *goredis.SliceCmd {
	return goredis.NewSliceResult(nil, e.err)
}

func (e ErrClient) HMSet(string, map[string]interface{}) *goredis.StatusCmd {
	return goredis.NewStatusResult("", e.err)
}

func (e ErrClient) HSet(key, field string, value interface{}) *goredis.BoolCmd {
	return goredis.NewBoolResult(false, e.err)
}

func (e ErrClient) LRange(key string, start, stop int64) *goredis.StringSliceCmd {
	return goredis.NewStringSliceResult(nil, e.err)
}

func (e ErrClient) MGet(keys ...string) *goredis.SliceCmd {
	return goredis.NewSliceResult(nil, e.err)
}

func (e ErrClient) MSet(pairs ...interface{}) *goredis.StatusCmd {
	return goredis.NewStatusResult("", e.err)
}

func (e ErrClient) Options() *goredis.Options {
	return nil
}

func (e ErrClient) Ping() *goredis.StatusCmd {
	return goredis.NewStatusResult("", e.err)
}

func (e ErrClient) RPopLPush(source string, destination string) *goredis.StringCmd {
	return goredis.NewStringResult("", e.err)
}

func (e ErrClient) RPush(key string, values ...interface{}) *goredis.IntCmd {
	return goredis.NewIntResult(0, e.err)
}

func (e ErrClient) SAdd(key string, members ...interface{}) *goredis.IntCmd {
	return goredis.NewIntResult(0, e.err)
}

func (e ErrClient) SCard(key string) *goredis.IntCmd {
	return goredis.NewIntResult(0, e.err)
}

func (e ErrClient) SIsMember(key string, member interface{}) *goredis.BoolCmd {
	return goredis.NewBoolResult(false, e.err)
}

func (e ErrClient) SMembers(key string) *goredis.StringSliceCmd {
	return goredis.NewStringSliceResult(nil, e.err)
}

func (e ErrClient) SPopN(key string, count int64) *goredis.StringSliceCmd {
	return goredis.NewStringSliceResult(nil, e.err)
}

func (e ErrClient) SRem(key string, members ...interface{}) *goredis.IntCmd {
	return goredis.NewIntResult(0, e.err)
}

func (e ErrClient) ScriptExists(scripts ...string) *goredis.BoolSliceCmd {
	return goredis.NewBoolSliceResult(nil, e.err)
}

func (e ErrClient) ScriptLoad(script string) *goredis.StringCmd {
	return goredis.NewStringResult("", e.err)
}

func (e ErrClient) Set(key string, value interface{}, expiration time.Duration) *goredis.StatusCmd {
	return goredis.NewStatusResult("", e.err)
}

func (e ErrClient) SetNX(key string, value interface{}, expiration time.Duration) *goredis.BoolCmd {
	return goredis.NewBoolResult(false, e.err)
}

func (e ErrClient) TTL(key string) *goredis.DurationCmd {
	return goredis.NewDurationResult(0, e.err)
}

func (e ErrClient) TxPipeline() goredis.Pipeliner {
	// TODO: should be nil?
	return nil
}

func (e *ErrClient) WithContext(context.Context) Client {
	return e
}

func (e ErrClient) ZAdd(key string, members ...goredis.Z) *goredis.IntCmd {
	return goredis.NewIntResult(0, e.err)
}

func (e ErrClient) ZCard(key string) *goredis.IntCmd {
	return goredis.NewIntResult(0, e.err)
}

func (e ErrClient) ZRangeByScore(key string, opt goredis.ZRangeBy) *goredis.StringSliceCmd {
	return goredis.NewStringSliceResult(nil, e.err)
}

func (e ErrClient) ZRangeByScoreWithScores(key string, opt goredis.ZRangeBy) *goredis.ZSliceCmd {
	return goredis.NewZSliceCmdResult(nil, e.err)
}

func (e ErrClient) ZRangeWithScores(key string, start, stop int64) *goredis.ZSliceCmd {
	return goredis.NewZSliceCmdResult(nil, e.err)
}

func (e ErrClient) ZRank(key, member string) *goredis.IntCmd {
	return goredis.NewIntResult(0, e.err)
}

func (e ErrClient) ZRem(key string, members ...interface{}) *goredis.IntCmd {
	return goredis.NewIntResult(0, e.err)
}

func (e ErrClient) ZRevRangeByScore(key string, opt goredis.ZRangeBy) *goredis.StringSliceCmd {
	return goredis.NewStringSliceResult(nil, e.err)
}

func (e ErrClient) ZRevRangeByScoreWithScores(key string, opt goredis.ZRangeBy) *goredis.ZSliceCmd {
	return goredis.NewZSliceCmdResult(nil, e.err)
}

func (e ErrClient) ZRevRangeWithScores(key string, start, stop int64) *goredis.ZSliceCmd {
	return goredis.NewZSliceCmdResult(nil, e.err)
}

func (e ErrClient) ZRevRank(key, member string) *goredis.IntCmd {
	return goredis.NewIntResult(0, e.err)
}

func (e ErrClient) ZScore(key, member string) *goredis.FloatCmd {
	return goredis.NewFloatResult(0, e.err)
}
