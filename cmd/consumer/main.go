package main

import (
	"fmt"

	"github.com/aoiito/sqs-fair-queue-verification/internal/consumer"
	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	handler := consumer.NewHandler(func(line string) { fmt.Println(line) })
	lambda.Start(handler.Handle)
}
