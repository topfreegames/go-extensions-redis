package redis_test

import (
	"testing"
	"time"

	goredis "github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	redis "github.com/topfreegames/go-extensions-redis"
)

func TestNewClient_Connect(t *testing.T) {
	opt, err := goredis.ParseURL("redis://localhost:6666")
	opt.DialTimeout = 20 * time.Millisecond
	assert.Nil(t, err)
	_, err = redis.NewClient(opt)
	assert.Nil(t, err)
}

func TestNewClient_Timeout(t *testing.T) {
	opt, err := goredis.ParseURL("redis://localhost:6660")
	opt.DialTimeout = 20 * time.Millisecond
	assert.Nil(t, err)
	_, err = redis.NewClient(opt)
	assert.Error(t, err, "timed out while waiting for Redis to connect")
}
