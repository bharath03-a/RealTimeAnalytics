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
