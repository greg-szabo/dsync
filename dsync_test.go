package main

import (
	"github.com/greg-szabo/dsync/ddb/sync"
	"github.com/stretchr/testify/assert"
	"testing"

	"fmt"
	"strconv"
	originalsync "sync"
	"time"
)

func Test_DDBLock_Default(t *testing.T) {
	m := sync.Mutex{}
	assert.NotPanics(t, m.Lock)
	assert.NotPanics(t, m.Unlock)
}

func Test_DDBLock_InitSettings(t *testing.T) {
	m := sync.Mutex{
		AWSRegion:    "us-west-1",
		DDBTableName: fmt.Sprintf("test-%d", time.Now().Unix()),
		Name:         "mykey",
	}
	assert.NotPanics(t, m.Lock)
	assert.NotPanics(t, m.Unlock)
}

func Test_DDBLock_ReuseConnection(t *testing.T) {
	TableName := fmt.Sprintf("Test-Reuse-%d", time.Now().Unix())
	m := sync.Mutex{DDBTableName: TableName}
	n := sync.Mutex{
		AWSSession:   m.AWSSession,
		DDBSession:   m.DDBSession,
		DDBTableName: TableName,
	}
	assert.NotPanics(t, m.Lock)
	assert.NotPanics(t, m.Unlock)
	assert.NotPanics(t, n.Lock)
	assert.NotPanics(t, n.Unlock)
}

func Test_DDBLock_TwoConnectionsAndADeadlock(t *testing.T) {
	TableName := fmt.Sprintf("Test-TwoPlusDead-%d", time.Now().Unix())
	m := sync.Mutex{DDBTableName: TableName}
	n := sync.Mutex{DDBTableName: TableName}
	assert.NotPanics(t, m.Lock)
	assert.Panics(t, n.Lock)
}

func Test_DDBLock_CannotUnlockOthers(t *testing.T) {
	TableName := fmt.Sprintf("Test-CannotUnlock-%d", time.Now().Unix())
	m := sync.Mutex{DDBTableName: TableName}
	n := sync.Mutex{DDBTableName: TableName}
	assert.NotPanics(t, m.Lock)
	assert.Panics(t, n.Unlock)
}

func Test_DDBLock_ParallelCount(t *testing.T) {
	TableName := fmt.Sprintf("Test-Parallel-%d", time.Now().Unix())
	thisMany := 10 // Do not raise this above 20 with the current timeout and capacity settings
	m := sync.Mutex{DDBTableName: TableName}
	wg := originalsync.WaitGroup{} // Oh, the irony...
	wg.Add(thisMany)
	for i := 0; i < thisMany; i++ {
		go func(m sync.Mutex) {
			defer wg.Done()
			m.Lock()
			defer m.Unlock()
			i, _ := strconv.Atoi(m.Value)
			m.Value = strconv.Itoa(i + 1)
		}(m)
	}
	wg.Wait()
	assert.Equal(t, strconv.Itoa(thisMany), m.LockAndGet())
	m.Unlock()
}

// Todo: Cleanup test tables at the end
