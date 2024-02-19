package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
)

var (
	topic           = flag.String("topic", "trial-L", "Topic name for publishing")
	slackWebhookURL = "https://hooks.slack.com/services/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"
)

type SlackMessage struct {
	Text string `json:"text"`
}

type Payload struct {
	Date        string  `json:"date"`
	Service     string  `json:"service"`
	Description string  `json:"description"`
	Cost        float64 `json:"cost"`
}

func main() {
	flag.Parse()
	projectId := "alphaus-live"
	ctx := context.Background()

	if *topic == "" {
		log.Println("topic cannot be empty")
		return
	}

	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		log.Println("NewClient failed:", err)
		return
	}

	defer client.Close()
	t := client.Topic(*topic)

	// Define different messages for different services
	messages := []string{
		`{"date":"2024-01-01","service":"AmazonEC2","description":"This is a sample description for Amazon EC2.","cost":1234.56}`,
		`{"date":"2024-01-02","service":"GoogleCloudStorage","description":"This is a sample description for Google Cloud Storage.","cost":789.12}`,
		`{"date":"2024-01-03","service":"AzureVM","description":"This is a sample description for Azure VM.","cost":567.89}`,
		`{"date":"2024-01-04","service":"DigitalOceanDroplet","description":"This is a sample description for DigitalOcean Droplet.","cost":345.67}`,
	}

	// Variables to store the state
	var (
		currentMessageIndex   int
		totalCost             float64
		totalCostMutex        sync.Mutex
		totalCostToDate       float64
		totalCostToDateMutex  sync.Mutex
		totalCostPerDate      map[string]float64
		totalCostPerDateMutex sync.Mutex
		totalMessages         int
		averageCostToDate     float64
		mutex                 sync.Mutex
	)

	totalCostPerDate = make(map[string]float64)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			log.Println("Received interrupt signal. Stopping publishing loop.")
			return
		case <-ticker.C:
			// Get the next message in the round-robin cycle
			msgData := messages[currentMessageIndex]

			result := t.Publish(ctx, &pubsub.Message{
				Data: []byte(msgData),
			})

			id, err := result.Get(ctx)
			if err != nil {
				log.Println("Get failed:", err)
				continue
			}

			log.Printf("Published message with ID: %v\n", id)

			// Update state variables
			mutex.Lock()
			totalMessages++
			mutex.Unlock()

			// Extract cost from message data
			var payload Payload
			if err := json.Unmarshal([]byte(msgData), &payload); err != nil {
				log.Println("Error parsing message payload:", err)
				continue
			}
			cost := payload.Cost

			// Update totalCost
			totalCostMutex.Lock()
			totalCost += cost
			totalCostMutex.Unlock()

			// Update totalCostToDate
			totalCostToDateMutex.Lock()
			totalCostToDate += cost
			totalCostToDateMutex.Unlock()

			// Update totalCostPerDate
			currentDate := payload.Date
			totalCostPerDateMutex.Lock()
			totalCostPerDate[currentDate] += cost
			totalCostPerDateMutex.Unlock()

			mutex.Lock()
			averageCostToDate = totalCostToDate / float64(totalMessages)
			mutex.Unlock()

			slackMsg := prepareSlackMessage(msgData, totalCost, totalMessages, totalCostToDate, averageCostToDate, totalCostPerDate)
			sendToSlack(slackMsg)

			// Move to the next message in the round-robin cycle
			currentMessageIndex = (currentMessageIndex + 1) % len(messages)
		}
	}
}

func prepareSlackMessage(msgData string, totalCost float64, totalMessages int, totalCostToDate float64, averageCostToDate float64, totalCostPerDate map[string]float64) string {
	slackMsg := SlackMessage{
		Text: "New Message Received:" + msgData + "\n" +
			"Total Messages: " + strconv.Itoa(totalMessages) + "\n" +
			"Total Cost: " + formatCost(totalCost) + "\n" +
			"Total Cost To Date: " + formatCost(totalCostToDate) + "\n" +
			"Average Cost To Date: " + formatCost(averageCostToDate) + "\n" +
			"Total Cost Per Date: " + formatTotalCostPerDate(totalCostPerDate),
	}

	slackPayload, err := json.Marshal(slackMsg)
	if err != nil {
		log.Println("Error marshalling Slack message:", err)
		return ""
	}

	return string(slackPayload)
}

func formatCost(cost float64) string {
	return "$" + strconv.FormatFloat(cost, 'f', 2, 64)
}

func formatTotalCostPerDate(totalCostPerDate map[string]float64) string {
	var result strings.Builder

	result.WriteString("\n")

	for date, cost := range totalCostPerDate {
		result.WriteString(fmt.Sprintf("%s: %s\n", date, formatCost(cost)))
	}

	return result.String()
}

func sendToSlack(message string) {
	resp, err := http.Post(slackWebhookURL, "application/json", strings.NewReader(message))
	if err != nil {
		log.Println("Error sending message to Slack:", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Println("Non-OK status code received from Slack:", resp.StatusCode)
	}
}
