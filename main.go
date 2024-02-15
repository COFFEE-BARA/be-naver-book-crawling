package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
)




func handler(ctx context.Context, event events.DynamoDBEvent) error {
	fmt.Print("함수 안에 들어오긴 했니?")
    for _, record := range event.Records {
        fmt.Printf("이벤트의 레코드 아이디 : %s\n", record.EventID)
        
        // Check the event name to determine the type of change
        switch string(record.EventName) {
        case string(events.DynamoDBOperationTypeInsert): // Check if the event is an insert operation
            // Access the new item from the NewImage attribute
            newImage := record.Change.NewImage
            fmt.Printf("새롭게 추가된 아이템 : %v\n", newImage)

            fmt.Printf("최신 크롤링 시간 : %s",newImage["crawling_time"])

            // newImage["crawling_time"]
            // now := time.Now()
            // fmt.Println(now)

            // crawlingTimeObj, ok := newImage["crawling_time"].(time.Time)
            // if !ok {
            //     fmt.Println("Error: crawling_time 은 타임 자료형이 아닙니다.")
                
            // }
            // fmt.Println(crawlingTimeObj)

        default:
            fmt.Printf("오잉: %s\n", record.EventName)
        }
    }
    
    return nil
}

func main() {
    lambda.Start(handler)
}