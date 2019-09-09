package redis

import (
	"context"
	"math/rand"
	"testing"
	"time"

	goredis "github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
)

func TestNewMux_DefaultOptions(t *testing.T) {
	cliopt, err := goredis.ParseURL("redis://localhost:6666")
	cliopt.DialTimeout = 20 * time.Millisecond
	assert.Nil(t, err)
	client, err := NewClient(cliopt)
	assert.Nil(t, err)
	muxopt := MuxOptions{
		HashClient: client,
		Clients:    []Client{client},
	}
	mux, err := NewMux(muxopt)
	assert.Nil(t, err)
	assert.Equal(t, mux.hashKeyPrefix, "hmk-")
	assert.Equal(t, int(mux.hashMapTTL), 0)
	assert.Equal(t, mux.withLockOnTTL, 3*time.Second)
	assert.Equal(t, mux.lockOptions.RetryBackoff, 5*time.Millisecond)
	assert.Equal(t, mux.lockOptions.RetryCount, 5)
}

func TestNewMux_HashClientAndClients(t *testing.T) {
	cliopt, err := goredis.ParseURL("redis://localhost:6666")
	cliopt.DialTimeout = 20 * time.Millisecond
	assert.Nil(t, err)
	client, err := NewClient(cliopt)
	assert.Nil(t, err)
	muxopt := MuxOptions{
		HashClient: client,
		Clients:    []Client{client},
	}
	mux, err := NewMux(muxopt)
	assert.Nil(t, err)
	assert.Equal(t, mux.hashClient, client)
	assert.Len(t, mux.clients, 1)
	assert.Equal(t, mux.clients[0], client)
}

func TestWithContext_NewMuxAndNewClientsWithCtx(t *testing.T) {
	cliopt, err := goredis.ParseURL("redis://localhost:6666")
	cliopt.DialTimeout = 20 * time.Millisecond
	assert.Nil(t, err)
	client, err := NewClient(cliopt)
	assert.Nil(t, err)
	muxopt := MuxOptions{
		HashClient: client,
		Clients:    []Client{client},
	}
	mux, err := NewMux(muxopt)
	assert.Nil(t, err)
	assert.Nil(t, mux.hashClient.(*BaseClient).ctx)
	assert.Len(t, mux.clients, 1)
	assert.Nil(t, mux.clients[0].(*BaseClient).ctx)
	ctx := context.Background()
	muxCtx := mux.WithContext(ctx)
	assert.NotEqual(t, muxCtx, mux)
	assert.Equal(t, muxCtx.(*BaseMux).hashClient.(*BaseClient).ctx, ctx)
	assert.Len(t, muxCtx.(*BaseMux).clients, 1)
	assert.Equal(t, muxCtx.(*BaseMux).clients[0].(*BaseClient).ctx, ctx)
}

func TestOn_InvalidateOnAgain(t *testing.T) {
	cli0opt, err := goredis.ParseURL("redis://localhost:6666")
	cli0opt.DialTimeout = 20 * time.Millisecond
	assert.Nil(t, err)
	cli1opt, err := goredis.ParseURL("redis://0.0.0.0:6666")
	cli1opt.DialTimeout = 20 * time.Millisecond
	assert.Nil(t, err)
	client0, err := NewClient(cli0opt)
	assert.Nil(t, err)
	client1, err := NewClient(cli1opt)
	assert.Nil(t, err)
	muxopt := MuxOptions{
		HashClient: client0,
		Clients:    []Client{client0, client1},
	}
	mux, err := NewMux(muxopt)
	assert.Nil(t, err)
	assert.Equal(t, mux.hashClient, client0)
	assert.Len(t, mux.clients, 2)
	assert.Equal(t, mux.clients[0], client0)
	assert.Equal(t, mux.clients[1], client1)
	cmd := client0.FlushAll()
	assert.Nil(t, cmd.Err())
	rand.Seed(25)
	hash := Hash("some_hash")
	// first call maps to client0
	someHashCli := mux.On(hash)
	assert.Equal(t, someHashCli, client0)
	// is cached during HashMapTTL
	assert.Equal(t, mux.onFromHashClient(hash), client0)
	// after invalidation, next calls can be mapped to != client
	err = mux.Invalidate(hash)
	assert.Nil(t, err)
	// due to rand.Seed(25), second call is hashed to client1
	someHashCli = mux.On(hash)
	assert.Equal(t, someHashCli, client1)
	// cached
	assert.Equal(t, mux.onFromHashClient(hash), client1)
	rand.Seed(time.Now().UnixNano())
}

func TestMux_OnManyShouldReuseFirstHashMapping(t *testing.T) {
	cli0opt, err := goredis.ParseURL("redis://localhost:6666")
	cli0opt.DialTimeout = 20 * time.Millisecond
	assert.Nil(t, err)
	cli1opt, err := goredis.ParseURL("redis://0.0.0.0:6666")
	cli1opt.DialTimeout = 20 * time.Millisecond
	assert.Nil(t, err)
	client0, err := NewClient(cli0opt)
	assert.Nil(t, err)
	client1, err := NewClient(cli1opt)
	assert.Nil(t, err)
	muxopt := MuxOptions{
		HashClient: client0,
		Clients:    []Client{client0, client1},
	}
	mux, err := NewMux(muxopt)
	assert.Nil(t, err)
	assert.Equal(t, mux.hashClient, client0)
	assert.Len(t, mux.clients, 2)
	assert.Equal(t, mux.clients[0], client0)
	assert.Equal(t, mux.clients[1], client1)
	cmd := client0.FlushAll()
	assert.Nil(t, cmd.Err())
	rand.Seed(25)
	hash := Hash("some_hash")
	many := []Hash{Hash("many_one")}
	// first call maps to client0
	someHashCli := mux.On(hash)
	assert.Equal(t, someHashCli, client0)
	// is cached during HashMapTTL
	assert.Equal(t, mux.onFromHashClient(hash), client0)
	assert.Equal(t, mux.OnMany(hash, many...), client0)
	rand.Seed(time.Now().UnixNano())
}

func TestMux_OnManyShouldOverrideManyMappings(t *testing.T) {
	cli0opt, err := goredis.ParseURL("redis://localhost:6666")
	cli0opt.DialTimeout = 20 * time.Millisecond
	assert.Nil(t, err)
	cli1opt, err := goredis.ParseURL("redis://0.0.0.0:6666")
	cli1opt.DialTimeout = 20 * time.Millisecond
	assert.Nil(t, err)
	client0, err := NewClient(cli0opt)
	assert.Nil(t, err)
	client1, err := NewClient(cli1opt)
	assert.Nil(t, err)
	muxopt := MuxOptions{
		HashClient: client0,
		Clients:    []Client{client0, client1},
	}
	mux, err := NewMux(muxopt)
	assert.Nil(t, err)
	assert.Equal(t, mux.hashClient, client0)
	assert.Len(t, mux.clients, 2)
	assert.Equal(t, mux.clients[0], client0)
	assert.Equal(t, mux.clients[1], client1)
	cmd := client0.FlushAll()
	assert.Nil(t, cmd.Err())
	rand.Seed(25)
	hash := Hash("some_hash")
	many := []Hash{Hash("many_one")}
	// first call maps to client0
	someHashCli := mux.On(hash)
	assert.Equal(t, someHashCli, client0)
	// first call maps to client1
	manyOneHashCli := mux.On(many[0])
	assert.Equal(t, manyOneHashCli, client1)
	// is cached during HashMapTTL
	assert.Equal(t, mux.onFromHashClient(hash), client0)
	assert.Equal(t, mux.onFromHashClient(many[0]), client1)
	// OnMany keeps `hash` (first arg) mapping and overrides `many` mappings
	allHashCli := mux.OnMany(hash, many...)
	assert.Equal(t, allHashCli, client0)
	assert.Equal(t, mux.onFromHashClient(hash), client0)
	assert.Equal(t, mux.onFromHashClient(many[0]), client0)
	rand.Seed(time.Now().UnixNano())
}
