package redis

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	goredis "github.com/go-redis/redis"
)

// Hash needs to be a SHORT and UNIQUE string in order for Mux.On to work
type Hash string

func (h Hash) String() string {
	return string(h)
}

// Mux is the minimal set of functions a redis multiplexer must implement
type Mux interface {
	On(Hash) Client
	OnMany(Hash, ...Hash) Client
	Invalidate(Hash) error
}

// BaseMux is an implementation of Mux where On and OnMany mappings are made to random clients
// if no existing mapping for `hash` is found
type BaseMux struct {
	addrClientMap map[string]Client
	addrs         []string
	clients       []Client
	hashClient    LockerClient
	hashKeyPrefix string
	hashMapTTL    time.Duration
	lenClients    int64
}

type MuxOptions struct {
	// HashClient is the client to an instance used to keep track of hashes assignments
	// in order to consistently return the same Client on Mux.On(Hash) calls
	HashClient LockerClient
	// Clients are all clients to which we wish to multiplex redis operations
	Clients []Client
	// HashMapTTL is the TTL for hash association with a redis client
	// Default: 0 - no TTL
	HashMapTTL time.Duration
	// HashKeyPrefix is the prefix added to each hash when mapping in HashClient
	HashKeyPrefix string
}

func NewMux(opt MuxOptions) (*BaseMux, error) {
	if err := opt.Validate(); err != nil {
		return nil, err
	}
	addrs := make([]string, len(opt.Clients))
	addrClientMap := map[string]Client{}
	for _, c := range opt.Clients {
		addr := c.Options().Addr
		addrClientMap[addr] = c
		addrs = append(addrs, addr)
	}
	return &BaseMux{
		addrClientMap: addrClientMap,
		addrs:         addrs,
		clients:       opt.Clients,
		hashClient:    opt.HashClient,
		hashKeyPrefix: opt.HashKeyPrefix,
		hashMapTTL:    opt.HashMapTTL,
		lenClients:    int64(len(opt.Clients)),
	}, nil
}

// WithContext returns a *BaseMux that runs operations under `ctx` and all its
// HashClient and []Client are also patched to run operations under `ctx`
func (m BaseMux) WithContext(ctx context.Context) *BaseMux {
	clients := make([]Client, 0, len(m.clients))
	for _, c := range m.clients {
		clients = append(clients, c.WithContext(ctx))
	}
	return &BaseMux{
		addrClientMap: m.addrClientMap,
		addrs:         m.addrs,
		clients:       clients,
		hashClient:    m.hashClient.WithContext(ctx).(LockerClient),
		hashKeyPrefix: m.hashKeyPrefix,
		hashMapTTL:    m.hashMapTTL,
		lenClients:    m.lenClients,
	}
}

// Validate MuxOptions
func (o MuxOptions) Validate() error {
	if o.HashClient == nil {
		return fmt.Errorf("HashClient is required")
	}
	if len(o.Clients) == 0 {
		return fmt.Errorf("at least one client is required")
	}
	return nil
}

// On guarantees that all operations for `hash` are executed on the same Client.
// If it fails, it returns a Client that fails for any request.
func (m BaseMux) On(hash Hash) Client {
	var client Client
	if err := m.WithLockOn(hash, func() { client = m.on(hash) }); err != nil {
		return NewErrClient(err)
	}
	return client
}

func (m BaseMux) on(hash Hash) Client {
	// is this hash already mapped to a client?
	if cli := m.onFromHashClient(hash); cli != nil {
		return cli
	}
	// if not: draw a random client and store the mapping
	draw := rand.Int63n(m.lenClients)
	if res := m.hashClient.Set(m.buildHashKey(hash), m.addrs[draw], m.hashMapTTL); res.Err() != nil {
		return NewErrClient(res.Err())
	}
	return m.clients[draw]
}

// OnMany guarantees that all operations for `hash` and `many` are executed on the same Client
// the first arg `hash` is really important here, it's the only that matters when verifying with
// an existing mapping to a Client already exists. For all the other `many`, it's mappings are
// overwritten with the one from `hash`.
// If it fails, it returns a Client that fails for any request.
func (m BaseMux) OnMany(hash Hash, many ...Hash) Client {
	var client Client
	err := m.WithLockOn(hash, func() {
		client = m.on(hash)
		addr := client.Options().Addr
		// TODO: MGet and check if they are already set to a different one?
		// TODO: MSetNX??
		// TODO: EXPIRATION (PIPE?)
		pairs := make([]interface{}, len(many)*2)
		for i := range many {
			pairs[2*i] = m.buildHashKey(many[i])
			pairs[2*i+1] = addr
		}
		if res := m.hashClient.MSet(pairs...); res.Err() != nil {
			client = NewErrClient(res.Err())
		}
	})
	if err != nil {
		return NewErrClient(err)
	}
	return client
}

// WithLockOn runs a func `f` under a unique lock for `hash`
func (m BaseMux) WithLockOn(hash Hash, f func()) error {
	lock, err := m.hashClient.Obtain(hash.String(), 3*time.Second, LockOptions{
		RetryCount:   5,
		RetryBackoff: 25 * time.Millisecond,
	})
	if err != nil {
		return err
	}
	defer lock.Release()
	f()
	return nil
}

// Tries to find an existing mapping of hash <-> Client.
// Returns `nil` if none exists.
func (m BaseMux) onFromHashClient(hash Hash) Client {
	strCmd := m.hashClient.Get(hash.String())
	err := strCmd.Err()
	if err != nil && err != goredis.Nil {
		return NewErrClient(err)
	}
	if err != nil && err == goredis.Nil {
		return nil
	}
	addr := strCmd.String()
	cli, ok := m.addrClientMap[addr]
	if !ok {
		return nil
	}
	return cli
}

// buildHashKey adds the BaseMux's hashKeyPrefix to hash.String()
func (m BaseMux) buildHashKey(hash Hash) string {
	return fmt.Sprintf("%s%s", m.hashKeyPrefix, hash.String())
}

// Invalidate removes the mapping for a hash
func (m BaseMux) Invalidate(hash Hash) error {
	return m.hashClient.Del(m.buildHashKey(hash)).Err()
}
