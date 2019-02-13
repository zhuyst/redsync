package redsync

import (
	"github.com/go-redis/redis"
	"testing"
	"time"
)

func TestMutex(t *testing.T) {
	redisClient := newMockRedisClient()
	mutex := newTestMutex(redisClient, "test-mutex")
	err := mutex.Lock()
	if err != nil {
		t.Fatalf("Expected err == nil, got %q", err)
	}
	defer mutex.Unlock()

	assertAcquired(t, redisClient, mutex)
}

func TestMutexExtend(t *testing.T) {
	redisClient := newMockRedisClient()
	mutex := newTestMutex(redisClient, "test-mutex-extend")

	err := mutex.Lock()
	if err != nil {
		t.Fatalf("Expected err == nil, got %q", err)
	}
	defer mutex.Unlock()

	time.Sleep(1 * time.Second)

	expiry := getPoolExpiry(redisClient, mutex.name)
	ok := mutex.Extend()
	if !ok {
		t.Fatalf("Expected ok == true, got %v", ok)
	}
	expiry2 := getPoolExpiry(redisClient, mutex.name)

	if expiry >= expiry2 {
		t.Fatalf("Expected expiry2 > expiry, got %d %d", expiry2, expiry)
	}
}

func newMockRedisClient() *redis.Client {
	redisClient := redis.NewClient(&redis.Options{
		Network:     "unix",
		Addr:        server.Socket(),
		IdleTimeout: 240 * time.Second,
	})

	if _, err := redisClient.Ping().Result(); err != nil {
		panic(err)
	}

	return redisClient
}

func getPoolValue(redisClient *redis.Client, name string) string {
	result, err := redisClient.Get(name).Result()
	if err != nil {
		panic(err)
	}

	return result
}

func getPoolExpiry(redisClient *redis.Client, name string) time.Duration {
	pttl, err := redisClient.PTTL(name).Result()
	if err != nil {
		panic(err)
	}

	return pttl
}

func newTestMutex(redisClient *redis.Client, name string) *Mutex {
	return &Mutex{
		name:        name,
		expiry:      8 * time.Second,
		tries:       32,
		delayFunc:   func(_ int) time.Duration { return 500 * time.Millisecond },
		factor:      0.01,
		redisClient: redisClient,
	}
}

func assertAcquired(t *testing.T, redisClient *redis.Client, mutex *Mutex) {
	value := getPoolValue(redisClient, mutex.name)
	if value != mutex.value {
		t.Fatalf("Expected value == %s, got %s", mutex.value, value)
	}
}
