package dataengineering

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.pubsub._
import com.spotify.scio.values._
import dataengineering.models.InteractionEvent
import dataengineering.models.Models
import dataengineering.models.Models.BuzzNotification
import dataengineering.options.PipelineArgs
import org.joda.time.Instant

import org.apache.beam.sdk.state.{StateSpecs, ValueState}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{Element, OutputReceiver, ProcessElement, StateId}
import org.apache.beam.sdk.values.KV
import java.lang.{Integer => JInt}
import org.apache.commons.lang3.NotImplementedException
import org.apache.beam.sdk.transforms.ParDo

// BigQuery imports
import com.spotify.scio.bigquery.types.BigQueryType
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}

object BuzzDetectionJob {

  // -------------------------
  // Stateful DoFn to track cumulative buzz count
  // -------------------------
  class BuzzStatefulDoFn extends DoFn[KV[String, Int], KV[String, Int]] {
    @StateId("buzzCount")
    private val buzzCountSpec = StateSpecs.value[JInt]()

    @ProcessElement
    def processElement(
        @Element element: KV[String, Int],
        out: OutputReceiver[KV[String, Int]],
        @StateId("buzzCount") buzzCount: ValueState[JInt]
    ): Unit = {
      val currentCount = Option(buzzCount.read()).map(_.intValue()).getOrElse(0)
      val updatedCount = currentCount + element.getValue
      buzzCount.write(updatedCount)
      out.output(KV.of(element.getKey, updatedCount))
    }
  }

  // -------------------------
  // Main pipeline
  // -------------------------
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val opts = PipelineArgs.buzzDetectionPipelineArgs(args)

    // Enable streaming
    sc.optionsAs[org.apache.beam.sdk.options.StreamingOptions].setStreaming(true)

    println("Starting Real-Time Reel Analytics Pipeline...")
    println("Features demonstrated:")
    println("  - Streaming from Pub/Sub")
    println("  - Side input for friend graph lookup")
    println("  - Stateful buzz detection")
    println("  - BigQuery integration")
    println("  - Debug outputs for monitoring")

    // -------------------------
    // SIDE INPUT: Load user graph
    // -------------------------
    val userGraphSideInput: SideInput[Map[String, List[String]]] = sc
      .textFile(opts.userGraphPath.get)
      .map { line =>
        val parts = line.split(",")
        (parts(0), parts(1)) // actor_id -> friend_id
      }
      .groupByKey
      .mapValues(_.toList)
      .asMapSideInput

    // -------------------------
    // STREAMING INPUT: Pub/Sub
    // -------------------------
    val interactionStream = sc.read(PubsubIO.string(opts.inputSubscription))(PubsubIO.ReadParam(PubsubIO.Subscription))
      .flatMap(json => _root_.io.circe.parser.decode[InteractionEvent](json).toOption)

    // -------------------------
    // FILTER: High-quality interactions
    // -------------------------
    val filteredStream = interactionStream
      .filter(event =>
        (event.event_type == "like" && event.watch_time_ms > 2000) ||
        event.event_type == "comment"
      )

    // -------------------------
    // ENRICHMENT: Expand to viewer-reel pairs via side input
    // -------------------------
    val enrichedStream = filteredStream
      .withSideInputs(userGraphSideInput)
      .flatMap { (interaction, side) =>
        val graph = side(userGraphSideInput)
        val friendsOfActor = graph.getOrElse(interaction.actor_id, Nil)
        friendsOfActor.map(friendId => s"$friendId:${interaction.reel_id}" -> 1)
      }
      .toSCollection

    // -------------------------
    // STATEFUL BUZZ COUNT: cumulative per viewer-reel
    // -------------------------
    val buzzCounts: SCollection[KV[String, Int]] = enrichedStream
      .map { case (key, count) => KV.of(key, count) }
      .applyTransform(ParDo.of(new BuzzStatefulDoFn))
      .filter(kv => kv.getValue >= 2) // threshold for alert

    // -------------------------
    // TRANSFORM TO NOTIFICATIONS
    // -------------------------
    val notifications = buzzCounts.map { kv =>
      val Array(viewerId, reelId) = kv.getKey.split(":")
      BuzzNotification(
        viewer_id = viewerId,
        reel_id = reelId,
        unique_friend_likes = kv.getValue,
        buzz_session_ended_at = Instant.now().toString
      )
    }

    // -------------------------
    // DEBUG OUTPUT
    // -------------------------
    notifications
      .map(n => s"BUZZ ALERT: User ${n.viewer_id} has ${n.unique_friend_likes} friends who interacted with reel ${n.reel_id}")
      .debug()

    // -------------------------
    // BIGQUERY OUTPUT: Save notifications
    // -------------------------
    notifications
      .map(BigQueryType[Models.BuzzNotification].toTableRow)
      .saveAsCustomOutput(
        "buzz-notifications-bq",
        BigQueryIO
          .writeTableRows()
          .to(opts.outputBQTable)
          .withSchema(BigQueryType[Models.BuzzNotification].schema)
          .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(WriteDisposition.WRITE_APPEND)
      )

    println("Pipeline configured successfully. Starting execution...")
    sc.run().waitUntilDone()
  }
}
