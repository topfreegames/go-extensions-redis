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
	All() []Client
	GetMapping(Hash) Client
	Invalidate(Hash) error
	InvalidateMany(...Hash) error
	On(Hash) Client
	OnMany(Hash, ...Hash) Client
	SaveMapping(client Client, hash Hash) Client
	SaveMappings(client Client, hash Hash, many ...Hash) Client
	WithContext(context.Context) Mux
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
	lockOptions   LockOptions
	withLockOnTTL time.Duration
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
	// Can't be empty
	// Default: "hmk-"
	HashKeyPrefix string
	// WithLockOnTTL is the TTL of the lock acquired by WithLockOn
	// Default: 3s
	WithLockOnTTL time.Duration
	// LockOptions are the LockOptions used by WithLockOn
	// In case of a nil an ExponentialBackoff will be used with
	// from 16ms to 64ms
	*LockOptions
}

func NewMux(opt MuxOptions) (*BaseMux, error) {
	if err := opt.Validate(); err != nil {
		return nil, err
	}
	addrs := make([]string, 0, len(opt.Clients))
	addrClientMap := make(map[string]Client, len(opt.Clients))
	for _, c := range opt.Clients {
		addr := c.Options().Addr
		addrClientMap[addr] = c
		addrs = append(addrs, addr)
	}
	if opt.HashKeyPrefix == "" {
		opt.HashKeyPrefix = "hmk-"
	}
	if opt.LockOptions == nil {
		options := DefaultLockOptions()
		opt.LockOptions = &options
	}
	if opt.WithLockOnTTL == 0 {
		opt.WithLockOnTTL = 3 * time.Second
	}
	return &BaseMux{
		addrClientMap: addrClientMap,
		addrs:         addrs,
		clients:       opt.Clients,
		hashClient:    opt.HashClient,
		hashKeyPrefix: opt.HashKeyPrefix,
		hashMapTTL:    opt.HashMapTTL,
		lenClients:    int64(len(opt.Clients)),
		lockOptions:   *opt.LockOptions,
		withLockOnTTL: opt.WithLockOnTTL,
	}, nil
}

func (m BaseMux) All() []Client {
	return m.clients
}

func (m BaseMux) GetMapping(hash Hash) Client {
	return m.onFromHashClient(hash)
}

// WithContext returns a *BaseMux that runs operations under `ctx` and all its
// HashClient and []Client are also patched to run operations under `ctx`
func (m BaseMux) WithContext(ctx context.Context) Mux {
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
		lockOptions:   m.lockOptions,
		withLockOnTTL: m.withLockOnTTL,
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
	client := m.clients[draw]
	return m.SaveMapping(client, hash)
}

// SaveMapping associates a hash to a client.
// BaseMux's implementation are called from methods that are holding a lock
// for custom implementations, do it under a lock as well (e.g WithLockOn)
func (m BaseMux) SaveMapping(client Client, hash Hash) Client {
	if res := m.hashClient.Set(m.buildHashKey(hash), client.Options().Addr, m.hashMapTTL); res.Err() != nil {
		return NewErrClient(res.Err())
	}
	return client
}

// OnMany guarantees that all operations for `hash` and `many` are executed on the same Client
// the first arg `hash` is really important here, it's the only that matters when verifying with
// an existing mapping to a Client already exists. For all the other `many`, their mappings are
// overwritten with the one from `hash`.
// If it fails, it returns a Client that fails for any request.
func (m BaseMux) OnMany(hash Hash, many ...Hash) Client {
	var client Client
	err := m.WithLockOn(hash, func() {
		client = m.on(hash)
		if _, ok := client.(*ErrClient); ok {
			// client is ErrClient, so don't save mappings
			return
		}
		client = m.SaveMappings(client, hash, many...)
	})
	if err != nil {
		return NewErrClient(err)
	}
	return client
}

// SaveMappings associates hashes to a client.
// SaveMappings BaseMux's implementation are called from methods that are holding a lock
// for custom implementations, do it under a lock as well (e.g WithLockOn)
func (m BaseMux) SaveMappings(client Client, hash Hash, many ...Hash) Client {
	addr := client.Options().Addr
	pipe := m.hashClient.TxPipeline()
	pipe.PExpire(m.buildHashKey(hash), m.hashMapTTL)
	pairs := make([]interface{}, len(many)*2)
	for i := range many {
		key := m.buildHashKey(many[i])
		pairs[2*i] = key
		pairs[2*i+1] = addr
		pipe.PExpire(key, m.hashMapTTL)
	}
	if res := m.hashClient.MSet(pairs...); res.Err() != nil {
		client = NewErrClient(res.Err())
	}
	if m.hashMapTTL > 0 {
		if _, err := pipe.Exec(); err != nil {
			client = NewErrClient(err)
		}
	}
	return client
}

// WithLockOn runs a func `f` under a unique lock for `hash`
func (m BaseMux) WithLockOn(hash Hash, f func()) error {
	lock, err := m.hashClient.Obtain(hash.String(), m.withLockOnTTL, LockOptions{
		MinTime: m.lockOptions.MinTime,
		MaxTime: m.lockOptions.MaxTime,
		Limit:   m.lockOptions.Limit,
	})
	if lock == nil {
		return fmt.Errorf("couldn't obtain lock for %v", hash)
	}
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
	key := m.buildHashKey(hash)
	strCmd := m.hashClient.Get(key)
	err := strCmd.Err()
	if err != nil && err != goredis.Nil {
		return NewErrClient(err)
	}
	if err == goredis.Nil {
		return nil
	}
	addr, err := strCmd.Result()
	if err != nil {
		return NewErrClient(err)
	}
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

// InvalidateMany removes the mapping for `many` hashes
func (m BaseMux) InvalidateMany(many ...Hash) error {
	if len(many) == 0 {
		return nil
	}
	keys := make([]string, len(many))
	for i := range many {
		keys[i] = m.buildHashKey(many[i])
	}
	return m.hashClient.Del(keys...).Err()
}

var _ Mux = (*BaseMux)(nil)
