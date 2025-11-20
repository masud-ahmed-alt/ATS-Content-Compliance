package lib

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// FailedBatch represents a batch that failed to be delivered to analyzer
type FailedBatch struct {
	RequestID  string      `json:"request_id"`
	MainURL    string      `json:"main_url"`
	BatchNum   int         `json:"batch_num"`
	Pages      []PageContent `json:"pages"`
	Error      string      `json:"error"`
	Timestamp  time.Time   `json:"timestamp"`
	RetryCount int         `json:"retry_count"`
}

// DeadLetterQueue manages failed batch delivery
type DeadLetterQueue struct {
	redisClient *redis.Client
	queueName   string
}

// NewDeadLetterQueue creates a new DLQ instance
func NewDeadLetterQueue(redisClient *redis.Client, queueName string) *DeadLetterQueue {
	if queueName == "" {
		queueName = "dlq:failed-batches"
	}
	return &DeadLetterQueue{
		redisClient: redisClient,
		queueName:   queueName,
	}
}

// Enqueue adds a failed batch to the dead letter queue
func (dlq *DeadLetterQueue) Enqueue(failedBatch FailedBatch) error {
	if dlq.redisClient == nil {
		log.Printf("[dlq:warning] Redis client unavailable, cannot enqueue failed batch %s", failedBatch.RequestID)
		return nil
	}

	data, err := json.Marshal(failedBatch)
	if err != nil {
		log.Printf("[dlq:error] Failed to marshal batch: %v", err)
		return err
	}

	// Add to Redis list with TTL tracking
	ctx := context.Background()
	err = dlq.redisClient.LPush(ctx, dlq.queueName, data).Err()
	if err != nil {
		log.Printf("[dlq:error] Failed to enqueue batch %s: %v", failedBatch.RequestID, err)
		return err
	}

	// Set TTL on queue (30 days)
	dlq.redisClient.Expire(ctx, dlq.queueName, 30*24*time.Hour)

	log.Printf(
		"[dlq:enqueued] Batch %s (retry_count=%d, error=%s)",
		failedBatch.RequestID, failedBatch.RetryCount, failedBatch.Error,
	)
	return nil
}

// Dequeue retrieves the next failed batch from the queue
func (dlq *DeadLetterQueue) Dequeue() (*FailedBatch, error) {
	if dlq.redisClient == nil {
		return nil, nil
	}

	ctx := context.Background()
	data, err := dlq.redisClient.RPop(ctx, dlq.queueName).Bytes()
	if err == redis.Nil {
		return nil, nil // Queue is empty
	}
	if err != nil {
		log.Printf("[dlq:error] Failed to dequeue: %v", err)
		return nil, err
	}

	var batch FailedBatch
	if err := json.Unmarshal(data, &batch); err != nil {
		log.Printf("[dlq:error] Failed to unmarshal batch: %v", err)
		return nil, err
	}

	return &batch, nil
}

// Length returns the number of items in the DLQ
func (dlq *DeadLetterQueue) Length() int64 {
	if dlq.redisClient == nil {
		return 0
	}

	ctx := context.Background()
	length, err := dlq.redisClient.LLen(ctx, dlq.queueName).Result()
	if err != nil {
		log.Printf("[dlq:error] Failed to get queue length: %v", err)
		return 0
	}

	return length
}

// Stats returns statistics about the DLQ
func (dlq *DeadLetterQueue) Stats() map[string]interface{} {
	return map[string]interface{}{
		"queue_name": dlq.queueName,
		"size":       dlq.Length(),
	}
}

// RetryFailedBatches attempts to retry all failed batches
func (dlq *DeadLetterQueue) RetryFailedBatches(ac *AnalyzerClient) error {
	for {
		batch, err := dlq.Dequeue()
		if err != nil {
			return err
		}
		if batch == nil {
			break // Queue is empty
		}

		// Convert back to PageBatch for sending
		pageBatch := PageBatch{
			RequestID:  batch.RequestID,
			MainURL:    batch.MainURL,
			BatchNum:   batch.BatchNum,
			Pages:      batch.Pages,
			IsComplete: true, // Mark as complete on retry
		}

		log.Printf("[dlq:retry] Retrying batch %s (attempt %d)", batch.RequestID, batch.RetryCount+1)

		err = ac.SendBatch(pageBatch)
		if err != nil {
			// Still failing - re-queue with incremented counter
			batch.RetryCount++
			batch.Error = err.Error()
			batch.Timestamp = time.Now()

			if batch.RetryCount < 5 { // Max 5 retries
				dlq.Enqueue(*batch)
				log.Printf("[dlq:requeue] Batch %s re-queued (retry_count=%d)", batch.RequestID, batch.RetryCount)
			} else {
				log.Printf("[dlq:failed] Batch %s exceeded max retries", batch.RequestID)
			}
		} else {
			log.Printf("[dlq:success] Batch %s successfully retried", batch.RequestID)
		}
	}

	return nil
}
