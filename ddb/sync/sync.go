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

type Mutex struct {
	initialized bool

	Name  string
	Value string
	id    int64

	AWSRegion     string
	AWSSession    *session.Session
	IgnoreEnvVars bool
	DDBSession    *dynamodb.DynamoDB
	DDBTableName  string
	timeout       time.Duration
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

	// Todo: Make the timeout configurable
	m.timeout = 10 * time.Second
	m.initialized = true
	return

}

func (m *Mutex) tryLock() (err error) {

	// Create lock in database
	condition := "attribute_not_exists(#name) OR attribute_not_exists(#id) OR #id = :zero OR #id = :id"

	result, err := m.DDBSession.UpdateItem(&dynamodb.UpdateItemInput{
		ConditionExpression: &condition,
		ExpressionAttributeNames: map[string]*string{
			"#name":      aws.String("Name"),
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
		},
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

func (m *Mutex) LockAndGet() string {
	m.Lock()
	return m.Value
}

func (m *Mutex) SetAndUnlock(value string) {
	m.Value = value
	m.Unlock()
}
