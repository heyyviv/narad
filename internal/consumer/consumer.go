package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/narad/narad/internal/storage"
	"github.com/redis/go-redis/v9"
)

type RedisConsumer struct {
	client *redis.Client
	store  *storage.Storage
	stream string
	group  string
	name   string
}

func NewRedisConsumer(redisURL string, store *storage.Storage) (*RedisConsumer, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opts)
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}

	return &RedisConsumer{
		client: client,
		store:  store,
		stream: "logiq:logs",
		group:  "logiq-workers",
		name:   uuid.New().String(),
	}, nil
}

func (c *RedisConsumer) Start(ctx context.Context) {
	err := c.client.XGroupCreateMkStream(ctx, c.stream, c.group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		log.Printf("Warning: failed to create consumer group: %v", err)
	}

	fmt.Printf("Worker %s started consuming stream %s...\n", c.name, c.stream)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			res, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    c.group,
				Consumer: c.name,
				Streams:  []string{c.stream, ">"},
				Count:    100,
				Block:    2 * time.Second,
			}).Result()

			if err != nil {
				if err == redis.Nil {
					continue
				}
				log.Printf("Error reading from redis: %v\n", err)
				time.Sleep(1 * time.Second)
				continue
			}

			if len(res) > 0 && len(res[0].Messages) > 0 {
				c.processMessages(ctx, res[0].Messages)
			}
		}
	}
}

func (c *RedisConsumer) processMessages(ctx context.Context, messages []redis.XMessage) {
	var logs []*storage.LogEvent
	var messageIDs []string

	now := time.Now()

	for _, msg := range messages {
		var payload string
		if p, ok := msg.Values["payload"].(string); ok {
			payload = p
		} else if len(msg.Values) == 1 {
			for _, v := range msg.Values {
				if vs, ok := v.(string); ok {
					payload = vs
					break
				}
			}
		} else {
			b, _ := json.Marshal(msg.Values)
			payload = string(b)
		}

		var logEvt storage.LogEvent
		if err := json.Unmarshal([]byte(payload), &logEvt); err != nil {
			log.Printf("Failed to unmarshal log message %s: %v", msg.ID, err)
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
		messageIDs = append(messageIDs, msg.ID)
	}

	if len(logs) > 0 {
		if err := c.store.InsertLogBatch(ctx, logs); err != nil {
			log.Printf("Failed to insert log batch: %v", err)
			return 
		}

		if err := c.client.XAck(ctx, c.stream, c.group, messageIDs...).Err(); err != nil {
			log.Printf("Failed to ack messages: %v", err)
		}
	} else if len(messageIDs) > 0 {
		c.client.XAck(ctx, c.stream, c.group, messageIDs...)
	}
}
