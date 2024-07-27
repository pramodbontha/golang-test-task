package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/streadway/amqp"
)

var (
	rdb     *redis.Client
	channel *amqp.Channel
	ctx     = context.Background()
)

type Message struct {
	Sender   string `json:"sender"`
	Receiver string `json:"receiver"`
	Message  string `json:"message"`
}

func main() {

	conn, err := amqp.Dial("amqp://user:password@localhost:7001/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	channel, err = conn.Channel()

	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}

	defer channel.Close()

	_, err = channel.QueueDeclare(
		"messages",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	// Connect to Redis
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	_, err = rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	go startSubscriber()

	r := gin.Default()

	r.GET("/test", func(c *gin.Context) {
		c.JSON(200, "worked")
	})

	r.POST("/message", postMessageHandler)
	r.GET("/message/list", getMessagesHandler)

	r.Run()
}

func startSubscriber() {
	msgs, err := channel.Consume(
		"messages",
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	for d := range msgs {
		var msg Message
		if err := json.Unmarshal(d.Body, &msg); err != nil {
			log.Fatalf("Failed to unmarshal message: %v", err)
			continue
		}

		timestamp := time.Now().UnixNano()
		key := msg.Sender + "_" + msg.Receiver
		rdb.ZAdd(ctx, key, &redis.Z{Score: float64(timestamp), Member: d.Body})
	}

}

func postMessageHandler(c *gin.Context) {
	var msg Message
	if err := c.BindJSON(&msg); err != nil {
		c.JSON(400, gin.H{"error": "Invalid request"})
		return
	}

	body, err := json.Marshal(msg)
	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to marshal message"})
		return
	}

	err = channel.Publish(
		"",
		"messages",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)

	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to send message"})
		return
	}

	c.JSON(200, gin.H{"message": "Message sent"})
}

func getMessagesHandler(c *gin.Context) {
	sender := c.Query("sender")
	receiver := c.Query("receiver")

	if sender == "" || receiver == "" {
		c.JSON(400, gin.H{"error": "Invalid request"})
		return
	}

	key := sender + "_" + receiver
	messages, err := rdb.ZRevRange(ctx, key, 0, -1).Result()
	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to get messages"})
		return
	}

	var messagesList []Message
	for _, msg := range messages {
		var message Message
		if err := json.Unmarshal([]byte(msg), &message); err != nil {
			c.JSON(500, gin.H{"error": "Failed to unmarshal message"})
			return
		}
		messagesList = append(messagesList, message)
	}

	c.JSON(200, gin.H{"messages": messagesList})
}
