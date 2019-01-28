package redsync

import (
	"crypto/rand"
	"encoding/base64"
	"github.com/go-redis/redis"
	"strings"
	"sync"
	"time"
)

// A DelayFunc is used to decide the amount of time to wait between retries.
type DelayFunc func(tries int) time.Duration

// A Mutex is a distributed mutual exclusion lock.
type Mutex struct {
	name   string
	expiry time.Duration

	tries     int
	delayFunc DelayFunc

	factor float64

	value string
	until time.Time

	nodem sync.Mutex

	redisClient *redis.Client
}

// Lock locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *Mutex) Lock() error {
	m.nodem.Lock()
	defer m.nodem.Unlock()

	value, err := m.genValue()
	if err != nil {
		return err
	}

	for i := 0; i < m.tries; i++ {
		if i != 0 {
			time.Sleep(m.delayFunc(i))
		}

		start := time.Now()

		acquireSuccess := m.acquire(value)

		until := time.Now().Add(m.expiry - time.Now().Sub(start) - time.Duration(int64(float64(m.expiry)*m.factor)) + 2*time.Millisecond)
		if acquireSuccess && time.Now().Before(until) {
			m.value = value
			m.until = until
			return nil
		}
		m.release(value)
	}

	return ErrFailed
}

// Unlock unlocks m and returns the status of unlock.
func (m *Mutex) Unlock() bool {
	m.nodem.Lock()
	defer m.nodem.Unlock()

	return m.release(m.value)
}

// Extend resets the mutex's expiry and returns the status of expiry extension.
func (m *Mutex) Extend() bool {
	m.nodem.Lock()
	defer m.nodem.Unlock()

	return m.touch(m.value, int(m.expiry/time.Millisecond))
}

func (m *Mutex) Acquire() bool {
	m.nodem.Lock()
	defer m.nodem.Unlock()

	value, err := m.genValue()
	if err != nil {
		return false
	}

	return m.acquire(value)
}

func (m *Mutex) genValue() (string, error) {
	b := make([]byte, 32)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func (m *Mutex) acquire(value string) bool {
	reply, err := m.redisClient.SetNX(m.name, value, m.expiry).Result()
	if err != nil {
		return false
	} else {
		return reply
	}
}

var deleteScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else
		return 0
	end
`)

func (m *Mutex) release(value string) bool {
	status, err := m.evalScript(deleteScript, m.name, value)
	return err == nil && status != 0
}

var touchScript = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("SET", KEYS[1], ARGV[1], "XX", "PX", ARGV[2])
	else
		return "ERR"
	end
`)

func (m *Mutex) touch(value string, expiry int) bool {
	status, err := m.evalScript(touchScript, m.name, value)
	return err == nil && status != "ERR"
}

func (m *Mutex) evalScript(script *redis.Script, key string, args ...interface{}) (interface{}, error) {
	keys := []string{m.name}

	status, err := script.EvalSha(m.redisClient, keys, args...).Result()
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT ") {
		status, err = script.Eval(m.redisClient, keys, args...).Result()
	}
	return status, err
}
