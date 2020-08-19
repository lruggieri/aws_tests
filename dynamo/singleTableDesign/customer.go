package main

import (
	"encoding/csv"
)

func FetchCustomer(reader *csv.Reader) ([]DynamoInsertItem, error){
	records, err := reader.ReadAll()
	if err != nil{
		return nil, err
	}
	customers := make([]DynamoInsertItem, len(records)-1)
	for i := 1 ; i < len(records) ; i++{
		newRow := make(DynamoInsertItem)
		for attributeIndex, attributeName := range records[0] {
			newRow[attributeName] = records[i][attributeIndex]
		}
		customers[i-1] = newRow
	}
	return customers, nil
}
