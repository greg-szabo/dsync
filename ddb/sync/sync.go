// Package sync (DynamoDB implementation) provides a distributed mutex that stores the semaphore in DynamoDB
// together with a value that can be shared across different processes.
package sync

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"math/rand"
	"os"
	"strconv"
	"time"
)

// A Mutex is a mutual exclusion lock.
// This version of a Mutex has extra properties for the AWS session and DynamoDB session details.
type Mutex struct {
	initialized bool

	// Name of the Mutex used in the DynamoDB table.
	Name string
	// Removed soon
	Value string
	// Amount of time before a locked mutex is considered abandoned.
	Expiry time.Duration
	id     int64

	// The AWS Region where the DynamoDB table resides.
	AWSRegion string
	// The AWS Session handle
	AWSSession *session.Session
	// Used to ignore AWS_* environment variables in favor of IAM policy permissions.
	// Use only if both are set up. By default, environment variables take precedence.
	IgnoreEnvVars bool
	// The DynamoDB Session handle
	DDBSession *dynamodb.DynamoDB
	// The DynamoDB Table name
	DDBTableName string
	timeout      time.Duration
	timeoutSet   bool
}

func (m *Mutex) initialization() (err error) {

	if m.initialized {
		return
	}

	// Defaults
	if m.AWSRegion == "" {
		m.AWSRegion = "us-east-1"
	}
	if m.DDBTableName == "" {
		m.DDBTableName = "Locks"
	}
	if m.Name == "" {
		m.Name = "Lock"
	}

	if m.Value == "" {
		m.Value = "0"
	}

	// Create AWS session, if it does not exist
	if m.AWSSession == nil {
		cfg := aws.Config{
			Region: aws.String(m.AWSRegion),
		}
		// Use IAM or environment variables credential
		if !m.IgnoreEnvVars &&
			((os.Getenv("AWS_ACCESS_KEY_ID") != "" && os.Getenv("AWS_SECRET_ACCESS_KEY") != "") ||
				(os.Getenv("AWS_ACCESS_KEY") != "" && os.Getenv("AWS_SECRET_KEY") != "")) {
			cfg.Credentials = credentials.NewEnvCredentials()
		}
		m.AWSSession = session.Must(session.NewSessionWithOptions(session.Options{Config: cfg}))
	}
	// Create DynamoDB session, if it does not exist
	if m.DDBSession == nil {
		m.DDBSession = dynamodb.New(m.AWSSession)
	}

	// Check table existence and create if not exists
	listTablesOutput, err := m.DDBSession.ListTables(&dynamodb.ListTablesInput{})
	found := false
	for item := range listTablesOutput.TableNames {
		if *listTablesOutput.TableNames[item] == m.DDBTableName {
			found = true
			break
		}
	}
	if !found {
		_, err := m.DDBSession.CreateTable(&dynamodb.CreateTableInput{
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String("Name"),
					AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
				},
			},
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String("Name"),
					KeyType:       aws.String(dynamodb.KeyTypeHash),
				},
			},
			// Todo: Make the capacity units configurable
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
			TableName: aws.String(m.DDBTableName),
		})
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				if aerr.Code() != dynamodb.ErrCodeResourceInUseException {
					panic(fmt.Sprintf("sync table not created: %v", err))
				}
			} else {
				panic(fmt.Sprintf("sync table not created: %v", err))
			}
		}
	}
	for {
		tableDescription, err := m.DDBSession.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(m.DDBTableName),
		})
		if *tableDescription.Table.TableStatus == dynamodb.TableStatusActive {
			break
		}
		if *tableDescription.Table.TableStatus == dynamodb.TableStatusCreating {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err == nil {
			err = errors.New(fmt.Sprintf("error activating table. Table status: %v", *tableDescription.Table.TableStatus))
		}
		panic(fmt.Sprintf("could not access table: %v", err.Error()))
	}

	rand.Seed(time.Now().UnixNano())
	for m.id == 0 {
		m.id = rand.Int63()
	}

	if !m.timeoutSet {
		m.timeout = 5 * time.Second
	}
	m.initialized = true
	return

}

func (m *Mutex) tryLock() (err error) {

	// Create lock in database
	condition := "attribute_not_exists(#name) OR attribute_not_exists(#id) OR #id = :zero OR #id = :id"
	expressionAttributeValues := map[string]*dynamodb.AttributeValue{
		":lastwrite": {
			N: aws.String(strconv.FormatInt(time.Now().UnixNano(), 10)),
		},
		":id": {
			N: aws.String(strconv.FormatInt(m.id, 10)),
		},
		":zero": {
			N: aws.String("0"),
		},
	}

	if m.Expiry > 0 {
		condition = condition + " OR ( #id <> :id AND #lastwrite < :nowminusexpiry )"
		expressionAttributeValues[":nowminusexpiry"] = &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatInt(time.Now().UnixNano()-m.Expiry.Nanoseconds(), 10)),
		}
	}

	result, err := m.DDBSession.UpdateItem(&dynamodb.UpdateItemInput{
		ConditionExpression: &condition,
		ExpressionAttributeNames: map[string]*string{
			"#name":      aws.String("Name"),
			"#lastwrite": aws.String("LastWrite"),
			"#id":        aws.String("LockerID"),
		},
		ExpressionAttributeValues: expressionAttributeValues,
		Key: map[string]*dynamodb.AttributeValue{
			"Name": {
				S: aws.String(m.Name),
			},
		},
		ReturnValues:     aws.String(dynamodb.ReturnValueAllNew),
		UpdateExpression: aws.String("SET #lastwrite=:lastwrite, #id=:id"),
		TableName:        &m.DDBTableName,
	})

	if err != nil {
		return
	}

	if value, ok := result.Attributes["Value"]; ok {
		m.Value = *value.S
	}

	return
}

func (m *Mutex) tryUnlock() (err error) {

	condition := "attribute_not_exists(#name) OR #id = :id"

	_, err = m.DDBSession.UpdateItem(&dynamodb.UpdateItemInput{
		ConditionExpression: &condition,
		ExpressionAttributeNames: map[string]*string{
			"#name":      aws.String("Name"),
			"#value":     aws.String("Value"),
			"#lastwrite": aws.String("LastWrite"),
			"#id":        aws.String("LockerID"),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":lastwrite": {
				N: aws.String(strconv.FormatInt(time.Now().UnixNano(), 10)),
			},
			":id": {
				N: aws.String(strconv.FormatInt(m.id, 10)),
			},
			":zero": {
				N: aws.String("0"),
			},
			":value": {
				S: aws.String(m.Value),
			},
		},
		Key: map[string]*dynamodb.AttributeValue{
			"Name": {
				S: aws.String(m.Name),
			},
		},
		UpdateExpression: aws.String("SET #lastwrite=:lastwrite, #id=:zero, #value=:value"),
		TableName:        &m.DDBTableName,
	})

	return
}

// WithTimeout defines a custom timeout value when trying to lock a key.
//
// Set it to 0 for no timeout.
//
// Default timeout value: 5 seconds
func (m Mutex) WithTimeout(timeout time.Duration) Mutex {
	m.timeout = timeout
	m.timeoutSet = true
	return m
}

// Lock locks the Mutex and retrieves its value from the database.
// If the lock is already in use, the calling goroutine blocks until the mutex is available
// or the timeout period has been reached.
//
// It ignores previous locks if an expiry period has been set. If the previous lock has expired, it immediately
// locks the lock.
func (m *Mutex) Lock() {
	m.initialization()
	started := time.Now().UnixNano()
	for {
		err := m.tryLock()
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
					if started < time.Now().UnixNano()-m.timeout.Nanoseconds() {
						panic(errors.New("could not lock mutex"))
					} else {
						time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
						continue
					}
				}
			}
			panic(err)
		} else {
			break
		}
	}
}

// Unlock writes the value into the database and unlocks the Mutex.
// It is a run-time error if the Mutex is not locked on entry to Unlock.
//
// A locked Mutex is associated with a particular Mutex variable.
// If a mutex expires, it is automatically considered unlocked.
func (m *Mutex) Unlock() {
	m.initialization()
	err := m.tryUnlock()
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				panic(errors.New("could not unlock mutex"))
			}
		}
		panic(err)
	}
}

// GetValueInt64 gets the value from the Mutex and returns it as an int64.
//
// It does not check if the Mutex was locked beforehand. An unlocked Mutex will return an out-of-sync result.
func (m *Mutex) GetValueInt64() int64 {

	if m.Value == "" {
		return 0
	}

	result, err := strconv.ParseInt(m.Value, 10, 64)
	if err != nil {
		panic(err.Error())
	}

	return result
}

// SetValueInt64 sets the int64 value in the Mutex. It does not check if the Mutex was locked beforehand. It does not write
// the value into the database. The value is written to the database during Unlock.
//
// See example(s) at GetValueInt64
func (m *Mutex) SetValueInt64(value int64) {
	m.Value = strconv.FormatInt(value, 10)
}

// GetValueString gets the value from the Mutex and returns it as a string.
//
// It does not check if the Mutex was locked beforehand. An unlocked Mutex will return an out-of-sync result.
func (m *Mutex) GetValueString() string {
	return m.Value
}

// SetValueString sets the string value in the Mutex. It does not check if the Mutex was locked beforehand. It does not write
// the value into the database. The value is written to the database during Unlock.
//
// See example(s) at GetValueString
func (m *Mutex) SetValueString(value string) {
	m.Value = value
}

// LockAndGetValueString is shorthand for locking the Mutex and retrieving its string value.
func (m *Mutex) LockAndGetValueString() string {
	m.Lock()
	return m.GetValueString()
}

// SetValueStringAndUnlock is shorthand for setting string the value in the Mutex and unlocking it.
func (m *Mutex) SetValueStringAndUnlock(value string) {
	m.SetValueString(value)
	m.Unlock()
}

// GetTimeout retrieves the timeout value set in the Mutex.
// Default value is 5 seconds.
func (m *Mutex) GetTimeout() time.Duration {
	return m.timeout
}
