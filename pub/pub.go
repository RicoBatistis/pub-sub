package pub

import (
	"context"
	"encoding/json"

	// "flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
)

// var (
// 	topicID = flag.String("topic", "trial-L", "Topic name for publishing")
// )

type Message struct {
	ID          string  `json:"id"`
	Date        string  `json:"date"`
	Service     string  `json:"service"`
	Description string  `json:"description"`
	Cost        float64 `json:"cost"`
}

func publisher(topicID string) {
	// flag.Parse()
	projectId := "alphaus-live"
	ctx := context.Background()

	if topicID == "" {
		log.Println("topic cannot be empty")
		return
	}

	// Create a Google Cloud Pub/Sub client
	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		log.Println("NewClient failed:", err)
		return
	}
	defer client.Close()

	// Create a Google Cloud Pub/Sub topic
	topic := client.Topic(topicID)

	// Listen for signals to gracefully stop message publishing
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// Start publishing messages in a loop
	for {
		select {
		case <-stop:
			log.Println("Received interrupt signal. Stopping message publishing.")
			return
		default:
			msg := generateMessage()
			data, err := json.Marshal(msg)
			if err != nil {
				log.Println("Error marshalling message:", err)
				continue
			}

			result := topic.Publish(ctx, &pubsub.Message{
				Data: data,
			})

			id, err := result.Get(ctx)
			if err != nil {
				log.Println("Get failed:", err)
				continue
			}

			log.Printf("Published message with ID: %v\n", id)

			time.Sleep(1 * time.Minute) // Publish a message every minute
		}
	}
}

func generateMessage() Message {
	return Message{
		ID:          generateUniqueID(),
		Date:        time.Now().Format("2006-01-02"),
		Service:     "SampleService",
		Description: "This is a sample description.",
		Cost:        123.45,
	}
}

func generateUniqueID() string {
	// Implement your logic to generate a unique ID here
	return time.Now().Format("20060102150405") // Example: timestamp as ID
}
