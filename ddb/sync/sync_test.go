package sync

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"testing"

	"fmt"
	"strconv"
	"sync"
	"time"
)

func DeleteTable(m Mutex) {
	m.DDBSession.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(m.DDBTableName),
	})
}

func Test_DDBLock_Default(t *testing.T) {
	m := Mutex{}
	assert.NotPanics(t, m.Lock)
	assert.NotPanics(t, m.Unlock)
}

func Test_DDBLock_InitSettings(t *testing.T) {
	m := Mutex{
		AWSRegion:    "us-west-1",
		DDBTableName: fmt.Sprintf("test-%d", time.Now().Unix()),
		Name:         "mykey",
	}
	assert.NotPanics(t, m.Lock)
	assert.NotPanics(t, m.Unlock)
	DeleteTable(m)
}

func Test_DDBLock_ReuseConnection(t *testing.T) {
	TableName := fmt.Sprintf("Test-Reuse-%d", time.Now().Unix())
	m := Mutex{DDBTableName: TableName}
	n := Mutex{
		AWSSession:   m.AWSSession,
		DDBSession:   m.DDBSession,
		DDBTableName: TableName,
	}
	assert.NotPanics(t, m.Lock)
	assert.NotPanics(t, m.Unlock)
	assert.NotPanics(t, n.Lock)
	assert.NotPanics(t, n.Unlock)
	DeleteTable(m)
}

func Test_DDBLock_TwoConnectionsAndADeadlock(t *testing.T) {
	TableName := fmt.Sprintf("Test-TwoPlusDead-%d", time.Now().Unix())
	m := Mutex{DDBTableName: TableName}
	n := Mutex{DDBTableName: TableName}
	assert.NotPanics(t, m.Lock)
	assert.Panics(t, n.Lock)
	DeleteTable(m)
}

func Test_DDBLock_CannotUnlockOthers(t *testing.T) {
	TableName := fmt.Sprintf("Test-CannotUnlock-%d", time.Now().Unix())
	m := Mutex{DDBTableName: TableName}
	n := Mutex{DDBTableName: TableName}
	assert.NotPanics(t, m.Lock)
	assert.Panics(t, n.Unlock)
	DeleteTable(m)
}

func Test_DDBLock_ParallelCount(t *testing.T) {
	TableName := fmt.Sprintf("Test-Parallel-%d", time.Now().Unix())
	thisMany := 10 // Do not raise this above 20 with the current timeout and capacity settings
	m := Mutex{DDBTableName: TableName}
	wg := sync.WaitGroup{} // Oh, the irony...
	wg.Add(thisMany)
	for i := 0; i < thisMany; i++ {
		go func(m Mutex) {
			defer wg.Done()
			assert.NotPanics(t, m.Lock)
			defer assert.NotPanics(t, m.Unlock)
			i := m.GetValueInt64()
			m.SetValueInt64(i + 1)
		}(m)
	}
	wg.Wait()
	assert.Equal(t, strconv.Itoa(thisMany), m.LockAndGetValueString())
	assert.NotPanics(t, m.Unlock)
	DeleteTable(m)
}

func Test_ValueTests(t *testing.T) {
	TableName := fmt.Sprintf("Test-Values-%d", time.Now().Unix())
	m := Mutex{DDBTableName: TableName}
	testValueInt64 := time.Now().Unix()
	assert.NotPanics(t, m.Lock)
	m.SetValueInt64(testValueInt64)
	assert.Equal(t, m.GetValueInt64(), testValueInt64)
	assert.Equal(t, m.GetValueString(), strconv.FormatInt(testValueInt64, 10))
	m.SetValueString(strconv.FormatInt(testValueInt64+1, 10))
	assert.Equal(t, m.GetValueInt64(), testValueInt64+1)
	assert.Equal(t, m.GetValueString(), strconv.FormatInt(testValueInt64+1, 10))
	assert.NotPanics(t, m.Unlock)
	DeleteTable(m)
}

func Test_Timeout(t *testing.T) {
	timeout := 2 * time.Second
	TableName := fmt.Sprintf("Test-Values-%d", time.Now().Unix())
	m := Mutex{DDBTableName: TableName}.WithTimeout(timeout)
	n := Mutex{DDBTableName: TableName}.WithTimeout(timeout)
	assert.NotPanics(t, m.Lock)
	assert.Equal(t, m.GetTimeout(), timeout)
	startTime := time.Now()
	assert.Panics(t, n.Lock)
	assert.Equal(t, n.GetTimeout(), timeout)
	endTime := time.Now()
	assert.Equal(t, endTime.Sub(startTime) > timeout, true)
	assert.Equal(t, endTime.Sub(startTime) < timeout+time.Second, true)
	assert.NotPanics(t, m.Unlock)
	DeleteTable(m)
}

func Test_Expiry(t *testing.T) {
	timeout := 1 * time.Second
	expiry := 3 * time.Second
	TableName := fmt.Sprintf("Test-Expiry-%d", time.Now().Unix())
	m := Mutex{DDBTableName: TableName, Expiry: expiry}.WithTimeout(timeout)
	n := Mutex{DDBTableName: TableName, Expiry: expiry}.WithTimeout(timeout)
	assert.NotPanics(t, m.Lock)
	assert.Panics(t, n.Lock)
	time.Sleep(expiry)
	startTime := time.Now()
	assert.NotPanics(t, n.Lock)
	endTime := time.Now()
	assert.Equal(t, endTime.Sub(startTime) < 1*time.Second, true)
	assert.Panics(t, m.Unlock)
	assert.NotPanics(t, n.Unlock)
	DeleteTable(m)
}

func ExampleMutex_Lock() {
	m := Mutex{}
	m.Lock()
	m.SetValueInt64(5)
	fmt.Print(m.GetValueInt64())
	m.Unlock()
	// Output: 5
}

func ExampleMutex_Lock_parameters() {
	m := Mutex{
		AWSRegion:    "us-west-1",
		DDBTableName: "MyTable",
		Name:         "thisismykey",
	}
	m.Lock()
	m.Unlock()
}

func ExampleMutex_Lock_reuseConnection() {
	m := Mutex{DDBTableName: "mytable"}
	n := Mutex{
		AWSSession:   m.AWSSession,
		DDBSession:   m.DDBSession,
		DDBTableName: m.DDBTableName,
	}
	m.Lock()
	m.Unlock()
	n.Lock()
	n.Unlock()
}

func ExampleMutex_Lock_expiry() {
	expiry := 2 * time.Second
	m := Mutex{Expiry: expiry}
	n := Mutex{Expiry: expiry}
	m.Lock()
	m.SetValueInt64(5)
	time.Sleep(expiry) // m expires

	n.Lock()
	n.SetValueInt64(6)
	n.Unlock() // Value written to database

	m.Lock() // Succeeds
	fmt.Print(m.GetValueInt64())
	m.Unlock()
	// Output: 6
}

func ExampleMutex_WithTimeout() {
	defaultTimeout := 5 * time.Second
	shorterTimeout := 2 * time.Second
	m := Mutex{}.WithTimeout(defaultTimeout)
	n := Mutex{}.WithTimeout(shorterTimeout)
	m.Lock()
	n.Lock() // panics after 2 seconds
}

func ExampleMutex_GetTimeout() {
	m := Mutex{}
	m.Lock()
	fmt.Printf("Default timeout: %d", m.GetTimeout())
	m.Unlock()
	// Output: Default timeout: 5000000000
}

func ExampleMutex_GetValueInt64() {
	m := Mutex{}
	m.Lock()
	m.SetValueInt64(4)
	fmt.Printf("Value: %d", m.GetValueInt64())
	m.Unlock()
	// Output: Value: 4
}

func ExampleMutex_GetValueString() {
	m := Mutex{}
	m.Lock()
	m.SetValueString("Hello World!")
	fmt.Printf("String value: %s", m.GetValueString())
	m.Unlock()
	// Output: String value: Hello World!
}

func ExampleMutex_LockAndGetValueString() {
	m := Mutex{}
	m.Lock()
	m.SetValueString("hello")
	m.Unlock()
	fmt.Printf(m.LockAndGetValueString())
	m.Unlock()
	// Output: hello
}
