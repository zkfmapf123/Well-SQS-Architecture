package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func main() {
	// AWS Config 로드
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("ap-northeast-2")) // 사용하고자 하는 리전 설정
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// SQS 클라이언트 생성
	svc := sqs.NewFromConfig(cfg)

	dlqURL := os.Getenv("DLQ_URL")
	normalQueueURL := os.Getenv("QUEUE_URL")

	// DLQ에서 메시지 수신
	receiveResp, err := svc.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
		QueueUrl:            &dlqURL,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     5,
	})
	if err != nil {
		log.Fatalf("failed to receive messages, %v", err)
	}

	if len(receiveResp.Messages) == 0 {
		fmt.Println("Not Exists")
		return
	}

	// 메시지가 있는 경우 처리
	for _, message := range receiveResp.Messages {
		fmt.Println(message)

		// 정상 큐로 메시지 전송 (fifo Queue는 다름)
		_, err := svc.SendMessage(context.TODO(), &sqs.SendMessageInput{
			QueueUrl:    &normalQueueURL,
			MessageBody: message.Body,
		})
		if err != nil {
			log.Printf("failed to send message to normal queue, %v", err)
			continue
		}

		// DLQ에서 메시지 삭제
		_, err = svc.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
			QueueUrl:      &dlqURL,
			ReceiptHandle: message.ReceiptHandle,
		})
		if err != nil {
			log.Printf("failed to delete message from DLQ, %v", err)
			continue
		}

		fmt.Printf("Message sent to normal queue and deleted from DLQ: %s\n", *message.Body)
	}

	if len(receiveResp.Messages) == 0 {
		fmt.Println("No messages to process.")
	}
}
