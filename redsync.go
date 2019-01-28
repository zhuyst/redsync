package redsync

import (
	"github.com/go-redis/redis"
	"time"
)

// RedSync provides a simple method for creating distributed mutexes using multiple Redis connection pools.
type RedSync struct {
	redisClient *redis.Client
}

// New creates and returns a new RedSync instance from given Redis connection pools.
func New(redisClient *redis.Client) *RedSync {
	return &RedSync{
		redisClient: redisClient,
	}
}

// NewMutex returns a new distributed mutex with given name.
func (r *RedSync) NewMutex(name string, options ...Option) *Mutex {
	m := &Mutex{
		name:        name,
		expiry:      8 * time.Second,
		tries:       32,
		delayFunc:   func(tries int) time.Duration { return 500 * time.Millisecond },
		factor:      0.01,
		redisClient: r.redisClient,
	}
	for _, o := range options {
		o.Apply(m)
	}
	return m
}

// An Option configures a mutex.
type Option interface {
	Apply(*Mutex)
}

// OptionFunc is a function that configures a mutex.
type OptionFunc func(*Mutex)

// Apply calls f(mutex)
func (f OptionFunc) Apply(mutex *Mutex) {
	f(mutex)
}

// SetExpiry can be used to set the expiry of a mutex to the given value.
func SetExpiry(expiry time.Duration) Option {
	return OptionFunc(func(m *Mutex) {
		m.expiry = expiry
	})
}

// SetTries can be used to set the number of times lock acquire is attempted.
func SetTries(tries int) Option {
	return OptionFunc(func(m *Mutex) {
		m.tries = tries
	})
}

// SetRetryDelay can be used to set the amount of time to wait between retries.
func SetRetryDelay(delay time.Duration) Option {
	return OptionFunc(func(m *Mutex) {
		m.delayFunc = func(tries int) time.Duration {
			return delay
		}
	})
}

// SetRetryDelayFunc can be used to override default delay behavior.
func SetRetryDelayFunc(delayFunc DelayFunc) Option {
	return OptionFunc(func(m *Mutex) {
		m.delayFunc = delayFunc
	})
}

// SetDriftFactor can be used to set the clock drift factor.
func SetDriftFactor(factor float64) Option {
	return OptionFunc(func(m *Mutex) {
		m.factor = factor
	})
}
