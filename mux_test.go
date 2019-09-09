package redis_test

import (
	"testing"
	"time"

	goredis "github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	redis "github.com/topfreegames/go-extensions-redis"
)

func TestMux_WithLockOn(t *testing.T) {
	cliopt, err := goredis.ParseURL("redis://localhost:6666")
	cliopt.DialTimeout = 20 * time.Millisecond
	assert.Nil(t, err)
	client, err := redis.NewClient(cliopt)
	assert.Nil(t, err)
	muxopt := redis.MuxOptions{
		HashClient: client,
		Clients:    []redis.Client{client},
	}
	mux, err := redis.NewMux(muxopt)
	assert.Nil(t, err)
	hash := redis.Hash("some_hash")
	ch := make(chan bool, 1)
	waitGo := make(chan bool, 1)
	go func() {
		err = mux.WithLockOn(hash, func() {
			waitGo <- true
			<-ch
		})
		assert.Nil(t, err)
	}()
	<-waitGo
	secondCallHappened := false
	err = mux.WithLockOn(hash, func() {
		secondCallHappened = true
	})
	assert.Error(t, err)
	assert.False(t, secondCallHappened)
	ch <- true
	err = mux.WithLockOn(hash, func() {
		secondCallHappened = true
	})
	assert.Nil(t, err)
	assert.True(t, secondCallHappened)
}

func TestMux_Invalidate(t *testing.T) {
	cliopt, err := goredis.ParseURL("redis://localhost:6666")
	cliopt.DialTimeout = 20 * time.Millisecond
	assert.Nil(t, err)
	client, err := redis.NewClient(cliopt)
	assert.Nil(t, err)
	muxopt := redis.MuxOptions{
		HashClient: client,
		Clients:    []redis.Client{client},
	}
	mux, err := redis.NewMux(muxopt)
	cmd := client.FlushAll()
	assert.Nil(t, cmd.Err())
	hash := redis.Hash("some_hash")
	hashCli := mux.On(hash)
	assert.Equal(t, hashCli, client)
	strCmd := client.Get("hmk-some_hash")
	assert.Nil(t, strCmd.Err())
	addr, err := strCmd.Result()
	assert.Nil(t, err)
	assert.Equal(t, addr, "localhost:6666")
	err = mux.Invalidate(hash)
	assert.Nil(t, err)
	strCmd = client.Get("hmk-some_hash")
	assert.Error(t, strCmd.Err(), "redis: nil")
	addr, err = strCmd.Result()
	assert.Equal(t, addr, "")
	assert.Error(t, err, "redis: nil")
}

func TestMux_InvalidateMany(t *testing.T) {
	cliopt, err := goredis.ParseURL("redis://localhost:6666")
	cliopt.DialTimeout = 20 * time.Millisecond
	assert.Nil(t, err)
	client, err := redis.NewClient(cliopt)
	assert.Nil(t, err)
	muxopt := redis.MuxOptions{
		HashClient: client,
		Clients:    []redis.Client{client},
	}
	mux, err := redis.NewMux(muxopt)
	cmd := client.FlushAll()
	assert.Nil(t, cmd.Err())
	hash0 := redis.Hash("some_hash_0")
	hash1 := redis.Hash("some_hash_1")
	hashCli := mux.OnMany(hash0, hash1)
	assert.Equal(t, hashCli, client)
	strCmd := client.Get("hmk-some_hash_0")
	assert.Nil(t, strCmd.Err())
	addr, err := strCmd.Result()
	assert.Nil(t, err)
	assert.Equal(t, addr, "localhost:6666")
	strCmd = client.Get("hmk-some_hash_1")
	assert.Nil(t, strCmd.Err())
	addr, err = strCmd.Result()
	assert.Nil(t, err)
	assert.Equal(t, addr, "localhost:6666")
	err = mux.InvalidateMany(hash0, hash1)
	assert.Nil(t, err)
	strCmd = client.Get("hmk-some_hash_0")
	assert.Error(t, strCmd.Err(), "redis: nil")
	addr, err = strCmd.Result()
	assert.Equal(t, addr, "")
	assert.Error(t, err, "redis: nil")
	strCmd = client.Get("hmk-some_hash_1")
	assert.Error(t, strCmd.Err(), "redis: nil")
	addr, err = strCmd.Result()
	assert.Equal(t, addr, "")
	assert.Error(t, err, "redis: nil")
}
