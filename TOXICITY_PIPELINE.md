# Real-Time Comment Toxicity Monitoring Pipeline

## Overview

This pipeline implements a real-time toxicity monitoring system that:

1. **Ingests comments in real-time** from Pub/Sub
2. **Runs ML-based toxicity classification** using a mock classifier
3. **Aggregates toxicity scores** at the post level over 5-minute windows
4. **Flags posts** where toxicity exceeds 30% threshold
5. **Stores results** in BigQuery for dashboards and alerts

## Architecture

```
Pub/Sub → Scio/Beam → ML Classification → Windowing → Aggregation → BigQuery
```

## Key Components

### 1. ML Toxicity Classifier (`ToxicityClassifierDoFn`)
- **Mock implementation** that simulates the `YamenRM/distilbert-toxic-comments` model
- Uses keyword-based detection with additional factors:
  - Toxic keywords: "hate", "stupid", "idiot", "moron", "kill", "die", etc.
  - Text patterns: CAPS ratio, exclamation marks
  - Random noise to simulate ML uncertainty
- Outputs toxicity scores (0.0 to 1.0)

### 2. Time-Windowed Aggregation
- **5-minute fixed windows** per post
- **Stateful aggregation** tracking:
  - Total comments in window
  - Toxic comments count
  - Toxicity ratio calculation
- **Threshold-based flagging**: Posts with >30% toxic comments are flagged

### 3. Data Models

#### Input: `CommentToxicityEvent`
```scala
case class CommentToxicityEvent(
  event_id: String,           // unique ID for the comment event
  event_timestamp: Long,      // event time in epoch millis
  user_id: String,            // who made the comment
  post_id: String,            // reel/post ID
  comment_text: String        // raw comment text
)
```

#### Output: `CommentToxicityNotification`
```scala
case class CommentToxicityNotification(
  post_id: String,           // reel/post ID
  window_start: String,      // window start timestamp (ISO format)
  window_end: String,        // window end timestamp (ISO format)
  total_comments: Long,      // number of comments in this window
  toxic_comments: Long,      // number flagged as toxic
  toxicity_ratio: Double,    // toxic_comments / total_comments
  flagged: Boolean           // whether post exceeds toxicity threshold
)
```

## Pipeline Flow

1. **Stream Ingestion**: Read from Pub/Sub subscription
2. **JSON Parsing**: Parse `CommentToxicityEvent` from JSON
3. **Toxicity Classification**: Apply ML model to get toxicity scores
4. **Windowing**: Group by post_id with 5-minute fixed windows
5. **Aggregation**: Calculate toxicity metrics per post per window
6. **Filtering**: Only output flagged posts (>30% toxicity)
7. **BigQuery Output**: Store results for dashboards and alerts

## Usage

### Running the Pipeline

```bash
sbt "runMain dataengineering.CommentToxicityJob \
  --inputSubscription=projects/your-project/subscriptions/comment-stream \
  --outputBQTable=your-project:dataset.toxicity_notifications \
  --dlqTopic=projects/your-project/topics/comment-dlq"
```

### Test Data

Sample test data is provided in `test-data/sample-toxicity-messages.json`:

```json
{"event_id": "evt_001", "event_timestamp": 1703001600000, "user_id": "user_123", "post_id": "post_456", "comment_text": "This is a great post! Love it!"}
{"event_id": "evt_002", "event_timestamp": 1703001660000, "user_id": "user_124", "post_id": "post_456", "comment_text": "I hate this stupid content, it's terrible!"}
```

## Configuration

### Pipeline Arguments
- `inputSubscription`: Pub/Sub subscription for comment events
- `outputBQTable`: BigQuery table for toxicity notifications
- `dlqTopic`: (Optional) Dead letter queue for failed messages
- `sideInputBQTable`: (Optional) Side input for additional data

### Thresholds
- **Toxicity threshold**: 30% (configurable)
- **Window size**: 5 minutes (configurable)
- **Toxicity score threshold**: 0.5 (for binary classification)

## Monitoring & Alerts

The pipeline outputs to BigQuery where you can:

1. **Create dashboards** showing toxicity trends
2. **Set up alerts** for posts exceeding thresholds
3. **Analyze patterns** in toxic content
4. **Track moderation** effectiveness

### Sample BigQuery Queries

```sql
-- Posts with highest toxicity in last hour
SELECT post_id, toxicity_ratio, total_comments, toxic_comments
FROM `your-project.dataset.toxicity_notifications`
WHERE window_end >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY toxicity_ratio DESC
LIMIT 10;

-- Toxicity trends over time
SELECT 
  DATE(window_end) as date,
  COUNT(*) as flagged_posts,
  AVG(toxicity_ratio) as avg_toxicity
FROM `your-project.dataset.toxicity_notifications`
WHERE window_end >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY DATE(window_end)
ORDER BY date;
```

## Integration with Real ML Model

To integrate with the actual `YamenRM/distilbert-toxic-comments` model:

1. **Replace the mock classifier** with a real ML service call
2. **Add model serving** (e.g., TensorFlow Serving, Vertex AI)
3. **Implement proper error handling** for model failures
4. **Add model versioning** and A/B testing capabilities

## Performance Considerations

- **Streaming processing**: Real-time with low latency
- **Stateful operations**: Efficient post-level aggregation
- **Windowing**: 5-minute windows balance latency vs. accuracy
- **BigQuery output**: Batched writes for efficiency
- **Error handling**: DLQ for failed messages

## Future Enhancements

1. **Real ML model integration**
2. **Dynamic threshold adjustment**
3. **Multi-language toxicity detection**
4. **Context-aware classification**
5. **Automated moderation actions**
6. **Real-time alerting systems**
