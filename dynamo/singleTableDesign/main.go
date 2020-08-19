package main

import (
	"aws/utils"
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	singleTableName = "single_table_test"
)

var logger = logrus.New()

func checkErr(err error){
	if err != nil {
		logger.Fatal(err)
	}
}

func main(){
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	dynamoClient := dynamodb.New(sess)

	checkErr(deleteDynamoDBTable(dynamoClient))

	// create the table if necessary
	checkErr(createDynamoTable(dynamoClient))

	logger.Info("Fetching csv data...")

	currentPath := utils.GetCallerPaths(1)[0]

	customersCSV, err := os.Open(filepath.Join(currentPath,"..","data","customers.csv"))
	checkErr(err)
	customersReader := csv.NewReader(customersCSV)
	customers, err := FetchCustomer(customersReader)
	checkErr(err)

	employeesCSV, err := os.Open(filepath.Join(currentPath,"..","data","employees.csv"))
	checkErr(err)
	employeesReader := csv.NewReader(employeesCSV)
	employees, err := FetchEmployee(employeesReader)
	checkErr(err)


	ordersCSV, err := os.Open(filepath.Join(currentPath,"..","data","orders.csv"))
	checkErr(err)
	ordersReader := csv.NewReader(ordersCSV)
	orders, err := FetchOrder(ordersReader)
	checkErr(err)

	cleanUpData(customers)
	cleanUpData(employees)
	cleanUpData(orders)

	logger.Info("Building adjacency list...")
	list, err := buildAdjacencyList(customers, employees, orders)
	checkErr(err)

	logger.Info("Inserting into DynamoDB...")
	checkErr(BatchInsert(dynamoClient, list))

	logger.Info("All done.")
}

func cleanUpData(data []DynamoInsertItem) {
	for _, item := range data {
		for itemKey, itemValue := range item {
			if strings.HasSuffix(itemKey, "ID") {
				/*
				adding whatever is before "ID" to the value.
				Example:
					productID: 12345 ==> productID: product#12345
				*/
				// TODO string concatenation is not performant
				item[itemKey] = itemKey[:len(itemKey)-2] + "#" + itemValue
			}
		}
	}
}

// insert into DynamoDB
func BatchInsert(client *dynamodb.DynamoDB, writeRequests []*dynamodb.WriteRequest) error{

	// actual insertion function
	insertIntoDynamo := func(batchCount int, writeRequests []*dynamodb.WriteRequest) error{
		logger.Infof("batch insertion #%d", batchCount)
		if len(writeRequests) == 0 {
			return nil
		}
		_, err := client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				singleTableName: writeRequests,
			},
		})
		if err != nil {
			return err
		}
		return nil
	}

	batchCount := 1
	for ; len(writeRequests) > 25 ; batchCount++ {
		err := insertIntoDynamo(batchCount, writeRequests[:25])
		if err != nil {
			return err
		}
		writeRequests = writeRequests[25:]
	}
	err := insertIntoDynamo(batchCount, writeRequests)
	if err != nil {
		return err
	}
	return nil
}

func isTableCreated(client *dynamodb.DynamoDB)(bool, error){
	tables, err := client.ListTables(&dynamodb.ListTablesInput{})
	if err != nil {
		return false, err
	}
	for _,tn := range tables.TableNames {
		if *tn == singleTableName {
			return true, nil
		}
	}
	return false, nil
}

// createDynamoTable creates the single table if not already present
func createDynamoTable(client *dynamodb.DynamoDB) error{
	// first, check if the table already exists
	tableIsCreated, err := isTableCreated(client)
	if err != nil{
		return err
	}
	if tableIsCreated {
		logger.Infof("Table '%s' already created, skipping creation", singleTableName)
		return nil
	}

	logger.Infof("Creating table %s", singleTableName)

	// now create the table
	tableDefinition := &dynamodb.CreateTableInput{
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("pk"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("sk"),
				KeyType:       aws.String("RANGE"),
			},
		},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("pk"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("sk"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("data"),
				AttributeType: aws.String("S"),
			},
		},
		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{
			{
				IndexName: aws.String("gsi_1"),
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String("sk"),
						KeyType: aws.String("HASH"),
					},
					{
						AttributeName: aws.String("data"),
						KeyType: aws.String("RANGE"),
					},
				},
				Projection: &dynamodb.Projection{
					ProjectionType: aws.String("ALL"),
				},
			},
		},
		BillingMode: aws.String("PAY_PER_REQUEST"),
		TableName: aws.String(singleTableName),
	}
	_, err = client.CreateTable(tableDefinition)
	checkErr(err)

	// wait for creation
	for {
		logger.Info("waiting for table creation...")
		tableIsCreated, err := isTableCreated(client)
		if err != nil{
			return err
		}
		if tableIsCreated {
			break
		}
		time.Sleep(time.Second)
	}
	timeForReadiness := 30 * time.Second
	logger.Infof("waiting %s for the table to be ready", timeForReadiness.String())
	time.Sleep(timeForReadiness)

	return nil
}

func deleteDynamoDBTable(client *dynamodb.DynamoDB) error {
	deleteInput := &dynamodb.DeleteTableInput{
		TableName: aws.String(singleTableName),
	}
	_, err := client.DeleteTable(deleteInput)
	if err != nil {
		return err
	}

	// waiting for table deletion
	for {
		logger.Info("waiting for table deletion...")
		tableIsCreated, err := isTableCreated(client)
		if err != nil{
			return err
		}
		if !tableIsCreated {
			break
		}
		time.Sleep(time.Second)
	}

	return nil
}

func buildAdjacencyList(customers, employees, orders []DynamoInsertItem) ([]*dynamodb.WriteRequest, error){
	list := make([]*dynamodb.WriteRequest, 0, len(customers) + len(employees) + len(orders))

	customersItems,err := getItemsToInsert(customers, "customerID", "contactName", true, []string{
		"country",
		"region",
		"city",
		"address",
	}...)
	if err != nil{
		return nil, fmt.Errorf("problem during insertion items creation for customers: %s", err.Error())
	}
	employeesItems,err := getItemsToInsert(employees, "employeeID", "reportsTo", true, "hireDate")
	if err != nil{
		return nil, fmt.Errorf("problem during insertion items creation for employees: %s", err.Error())
	}
	ordersItems,err := getItemsToInsert(orders, "orderID", "ORDER", false, "orderDate")
	if err != nil{
		return nil, fmt.Errorf("problem during insertion items creation for orders: %s", err.Error())
	}

	list = append(list, customersItems...)
	list = append(list, employeesItems...)
	list = append(list, ordersItems...)

	return list, nil
}

func getItemsToInsert(insertItems []DynamoInsertItem, pk, sk string, skMandatory bool, gs1Sks ...string) ([]*dynamodb.WriteRequest, error){
	writeRequests := make([]*dynamodb.WriteRequest, 0, len(insertItems))
	for idx, insertItem := range insertItems {
		// check item validity
		if _,ok := insertItem[pk] ; !ok {
			return nil, fmt.Errorf("missing pk '%s' on item %d", pk, idx)
		}
		skValue := sk
		if skMandatory {
			if _,ok := insertItem[sk] ; !ok {
				return nil, fmt.Errorf("missing sk '%s' on item %d", sk, idx)
			}
			skValue = insertItem[sk]
		}

		var gs1String bytes.Buffer
		for idx,gs1Sk := range gs1Sks {
			if _,ok := insertItem[gs1Sk] ; !ok {
				return nil, fmt.Errorf("missing gs1Sk key %s on item %d", gs1Sk, idx)
			}
			gs1String.WriteString(insertItem[gs1Sk])
			if idx > 0 && idx != len(gs1Sks)-1{
				gs1String.WriteRune('#')
			}
		}

		newItem := &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"pk":{
						S: aws.String(insertItem[pk]),
					},
					"sk":{
						S: aws.String(skValue),
					},
					"data":{
						S: aws.String(gs1String.String()),
					},
				},
			},
		}
		writeRequests = append(writeRequests, newItem)
	}

	return writeRequests, nil
}


