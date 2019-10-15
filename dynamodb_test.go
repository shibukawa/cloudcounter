// +build dynamodb

package cloudcounter

import (
	"context"
	"gocloud.dev/docstore/awsdynamodb"
	"log"
	"os"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
	//"os"
	"os/exec"
	//"syscall"
	"testing"
)

func TestMain(m *testing.M) {
	dynamoLocal := exec.Command("docker", "run", "--name", "dynamodb-local", "--rm", "-p", "8000:8000", "amazon/dynamodb-local")
	log.Println(dynamoLocal.String())
	err := dynamoLocal.Start()
	if err != nil {
		panic(err)
	}
	time.Sleep(3 * time.Second)
	db := openDB()

	log.Println("DeleteTable")
	_, err = db.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String("counter-table"),
	})

	log.Println("CreateTable")
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("id"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("id"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		TableName: aws.String("CounterTable"),
	}

	_, err = db.CreateTable(input)
	if err != nil {
		panic(err)
	}

	result := m.Run()
	log.Println("sending SIGTERM...")
	dynamoLocal.Process.Signal(syscall.SIGTERM)
	os.Exit(result)
}

func openDB() *dynamodb.DynamoDB {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String("ap-northeast-1"),
		Endpoint:    aws.String("http://localhost:8000"),
		Credentials: credentials.NewStaticCredentials("dummy", "dummy", "dummy"),
	}))
	db := dynamodb.New(sess)
	return db
}

func TestDynamoIncrement(t *testing.T) {
	coll, err := awsdynamodb.OpenCollection(openDB(), "CounterTable", "id", "", nil)
	assert.Nil(t, err)
	if err != nil {
		t.Log(err.Error())
		return
	}
	var testKey CounterKey = "test"

	counter := NewCounter(coll)
	err = counter.Register(context.Background(), testKey)
	assert.Nil(t, err)
	count, err := counter.Get(context.Background(), testKey)
	assert.Nil(t, err)
	assert.Equal(t, 0, count)

	eg := errgroup.Group{}

	for i := 0; i < 100; i++ {
		eg.Go(func() error {
			counter.Increment(context.Background(), testKey)
			return nil
		})
	}
	eg.Wait()

	count, err = counter.Get(context.Background(), testKey)
	assert.Nil(t, err)
	assert.Equal(t, 100, count)
}
