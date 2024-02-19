package sub

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var ()

type Message struct {
	ID          int64            `json:"id"`
	Date        spanner.NullDate `json:"date"`
	Service     string           `json:"service"`
	Description string           `json:"description"`
	Cost        float64          `json:"cost"`
}

func () {
	flag.Parse()
	projectId := "alphaus-live"
	ctx := context.Background()

	spannerClient, err := spanner.NewClient(ctx, "projects/"+projectId+"/instances/intern2024ft/databases/default", option.WithCredentialsFile(`"D:\Alp\internship202401svcacct.json"`))
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer spannerClient.Close()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// Fetch data from Spanner and print in JSON format
	err = fetchDataAndPrint(ctx, spannerClient)
	if err != nil {
		log.Fatalf("Error fetching data: %v", err)
	}

	<-stop
	log.Println("Received interrupt signal. Exiting.")
}

func fetchDataAndPrint(ctx context.Context, client *spanner.Client) error {
	iter := client.Single().Read(ctx, "jet_tbl", spanner.AllKeys(), []string{"id", "date", "service", "description", "cost"})

	var messages []Message

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		var msg Message
		if err := row.ToStruct(&msg); err != nil {
			return err
		}

		messages = append(messages, msg)
	}

	// Convert messages slice to JSON and print to terminal
	jsonData, err := json.Marshal(messages)
	if err != nil {
		return err
	}

	log.Println(string(jsonData))
	return nil
}
