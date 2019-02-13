package redsync

import (
	"os"
	"testing"

	"github.com/stvp/tempredis"
)

var server *tempredis.Server

func TestMain(m *testing.M) {
	server, err := tempredis.Start(tempredis.Config{})
	if err != nil {
		panic(err)
	}
	result := m.Run()
	server.Term()
	os.Exit(result)
}

func TestRedsync(t *testing.T) {
	redisClient := newMockRedisClient()
	rs := New(redisClient)

	mutex := rs.NewMutex("test-redsync")
	err := mutex.Lock()
	if err != nil {

	}

	assertAcquired(t, redisClient, mutex)
}
