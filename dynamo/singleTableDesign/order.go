package main

import (
	"encoding/csv"
)

func FetchOrder(reader *csv.Reader) ([]DynamoInsertItem, error){
	records, err := reader.ReadAll()
	if err != nil{
		return nil, err
	}
	orders := make([]DynamoInsertItem, len(records)-1)
	for i := 1 ; i < len(records) ; i++{
		newRow := make(DynamoInsertItem)
		for attributeIndex, attributeName := range records[0] {
			newRow[attributeName] = records[i][attributeIndex]
		}
		orders[i-1] = newRow
	}
	return orders, nil
}