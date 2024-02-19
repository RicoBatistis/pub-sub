package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"
)

var (
	subId = flag.String("subscription", "trial-L1", "Subscription name")
)

type Message struct {
	ID          string  `json:"id"`
	Date        string  `json:"date"`
	Service     string  `json:"service"`
	Description string  `json:"description"`
	Cost        float64 `json:"cost"`
}

func main() {
	flag.Parse()
	projectId := "alphaus-live"
	ctx := context.Background()

	if *subId == "" {
		log.Println("subscription cannot be empty")
		return
	}

	// Create a Google Cloud Pub/Sub client
	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		log.Println("NewClient failed:", err)
		return
	}
	defer client.Close()

	sub := client.Subscription(*subId)
	sub.ReceiveSettings.Synchronous = true

	// Create a Google Cloud Spanner client
	spannerClient, err := spanner.NewClient(ctx, "projects/"+projectId+"/instances/intern2024ft/databases/default", option.WithCredentialsFile(`C:\Users\user\Internship\internship202401svcacct.json`))
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer spannerClient.Close()

	// Listen for signals to gracefully stop message processing
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// Start receiving messages in a loop
	err = sub.Receive(ctx, func(_ context.Context, msg *pubsub.Message) {
		log.Printf("Received: %q", msg)
		processMessage(ctx, spannerClient, msg.Data)
		msg.Ack()
	})

	if err != nil {
		log.Println("Receive failed:", err)
		return
	}
	<-stop
	log.Println("Received interrupt signal. Stopping message processing.")
}

func processMessage(ctx context.Context, client *spanner.Client, data []byte) {
	var msg Message
	err := json.Unmarshal(data, &msg)
	if err != nil {
		log.Println("Error unmarshalling message:", err)
		return
	}

	msg.ID = generateUniqueID() // Generate unique ID for the message

	// Insert data into Spanner
	if err := insertDataIntoSpanner(ctx, client, msg); err != nil {
		log.Printf("Error inserting data into Spanner: %v", err)
	}
}

func insertDataIntoSpanner(ctx context.Context, client *spanner.Client, msg Message) error {
	mutation := spanner.InsertOrUpdate("jet_tbl",
		[]string{"id", "date", "service", "description", "cost"},
		[]interface{}{msg.ID, msg.Date, msg.Service, msg.Description, msg.Cost})

	_, err := client.Apply(ctx, []*spanner.Mutation{mutation})
	return err
}

func generateUniqueID() string {
	// Implement your logic to generate a unique ID here
	return time.Now().Format("20060102150405") // Example: timestamp as ID
}
