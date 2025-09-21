package dataengineering.models

import com.spotify.scio.bigquery.types._
import io.circe.generic.semiauto._
import io.circe.{Decoder, Encoder}

// Input from Pub/Sub
case class InteractionEvent(
                             event_id: String,
                             event_timestamp: Long,
                             event_type: String,
                             actor_id: String,
                             reel_id: String,
                             author_id: String,
                             watch_time_ms: Long,
                             surface: String
                           )

object InteractionEvent {
  implicit val decoder: Decoder[InteractionEvent] = deriveDecoder[InteractionEvent]
  implicit val encoder: Encoder[InteractionEvent] = deriveEncoder[InteractionEvent]
}

// Final output for BigQuery
object Models {
  @BigQueryType.toTable
  case class BuzzNotification(
                               viewer_id: String,
                               reel_id: String,
                               unique_friend_likes: Int,
                               buzz_session_ended_at: String
                             )
}

// Comment Toxcity Models

// Input Event Schema (Pub/Sub -> Beam)
case class CommentToxicityEvent(
  event_id: String,           // unique ID for the comment event
  event_timestamp: Long,      // event time in epoch millis
  user_id: String,            // who made the comment
  post_id: String,            // reel/post ID
  comment_text: String        // raw comment text
)

object CommentToxicityModels {
  @BigQueryType.toTable
  case class CommentToxicityNotification(
    post_id: String,           // reel/post ID
    window_start: String,      // window start timestamp (ISO format)
    window_end: String,        // window end timestamp (ISO format)
    total_comments: Long,      // number of comments in this window
    toxic_comments: Long,      // number flagged as toxic
    toxicity_ratio: Double,    // toxic_comments / total_comments
    flagged: Boolean           // whether post exceeds toxicity threshold
  )
}
