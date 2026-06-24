package consumer

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/narad/narad/internal/config"
	"github.com/narad/narad/internal/storage"
	"github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
	reader *kafka.Reader
	store  *storage.Storage
	topic  string
	group  string
}

func NewKafkaConsumer(cfg *config.Config, store *storage.Storage) *KafkaConsumer {
	brokers := strings.Split(cfg.KafkaBrokers, ",")
	for i, b := range brokers {
		brokers[i] = strings.TrimSpace(b)
	}

	topic := cfg.KafkaTopic
	if topic == "" {
		return nil
	}
	group := cfg.KafkaGroup
	if group == "" {
		return nil
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  group,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  1 * time.Second,
	})

	return &KafkaConsumer{
		reader: r,
		store:  store,
		topic:  topic,
		group:  group,
	}
}

func (c *KafkaConsumer) Start(ctx context.Context) {
	log.Printf("Worker started consuming Kafka topic %s...\n", c.topic)

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping Kafka consumer...")
			c.reader.Close()
			return
		default:
			// Fetch a message
			m, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Error fetching from Kafka: %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			// Accumulate a batch of messages up to 100ms or 100 messages
			messages := []kafka.Message{m}
			batchCtx, batchCancel := context.WithTimeout(ctx, 100*time.Millisecond)

			for len(messages) < 100 {
				nm, err := c.reader.FetchMessage(batchCtx)
				if err != nil {
					break
				}
				messages = append(messages, nm)
			}
			batchCancel()

			// Process the batch
			c.processBatch(ctx, messages)
		}
	}
}

func (c *KafkaConsumer) processBatch(ctx context.Context, messages []kafka.Message) {
	var logs []*storage.LogEvent
	now := time.Now()

	for _, msg := range messages {
		var logEvt storage.LogEvent
		if err := json.Unmarshal(msg.Value, &logEvt); err != nil {
			log.Printf("Failed to unmarshal Kafka log message: %v", err)
			continue
		}

		if logEvt.ID == uuid.Nil {
			logEvt.ID = uuid.New()
		}
		if logEvt.Ts.IsZero() {
			logEvt.Ts = now
		}
		logEvt.ReceivedAt = now
		if logEvt.Service == "" {
			logEvt.Service = "unknown"
		}
		if logEvt.Level == "" {
			logEvt.Level = "INFO"
		}
		logEvt.Tier = 1
		logEvt.Confidence = 1.0

		logs = append(logs, &logEvt)
	}

	if len(logs) > 0 {
		if err := c.store.InsertLogBatch(ctx, logs); err != nil {
			log.Printf("Failed to insert log batch from Kafka: %v", err)
			return
		}

		// Commit the offsets
		if err := c.reader.CommitMessages(ctx, messages...); err != nil {
			log.Printf("Failed to commit Kafka offsets: %v", err)
		}
	} else if len(messages) > 0 {
		// Commit empty/skipped messages
		c.reader.CommitMessages(ctx, messages...)
	}
}
