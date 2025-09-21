package dataengineering

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.bigquery._
import com.spotify.scio.values.SCollection
import dataengineering.options.PipelineArgs
import dataengineering.models.{CommentToxicityEvent, CommentToxicityModels}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.state.{StateSpecs, ValueState}
import org.apache.beam.sdk.transforms.windowing.{FixedWindows, Window}
import org.joda.time.Duration
import java.lang.{Integer => JInt, Double => JDouble, Boolean => JBoolean}
import com.spotify.scio.pubsub.PubsubIO
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import scala.util.Random
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition

object CommentToxicityJob {
  
  // -------------------------
  // ML Model Simulation (Mock Toxicity Classifier)
  // -------------------------
  class ToxicityClassifierDoFn extends DoFn[CommentToxicityEvent, (CommentToxicityEvent, Double)] {
    @DoFn.ProcessElement
    def processElement(c: DoFn[CommentToxicityEvent, (CommentToxicityEvent, Double)]#ProcessContext): Unit = {
      val event = c.element()
      
      // Mock toxicity classification based on keywords and patterns
      val toxicityScore = classifyToxicity(event.comment_text)
      
      c.output((event, toxicityScore))
    }
    
    private def classifyToxicity(text: String): Double = {
      val lowerText = text.toLowerCase
      
      // Simple keyword-based toxicity detection (mock implementation)
      val toxicKeywords = Seq(
        "hate", "stupid", "idiot", "moron", "kill", "die", "worst", "terrible",
        "awful", "disgusting", "pathetic", "loser", "trash", "garbage"
      )
      
      val toxicCount = toxicKeywords.count(keyword => lowerText.contains(keyword))
      val length = text.length
      
      // Base toxicity score from keywords
      val keywordScore = if (toxicCount > 0) 0.3 + (toxicCount * 0.1) else 0.0
      
      // Additional factors
      val capsRatio = text.count(_.isUpper).toDouble / length
      val exclamationRatio = text.count(_ == '!').toDouble / length
      
      // Combine factors
      val baseScore = keywordScore + (capsRatio * 0.2) + (exclamationRatio * 0.1)
      
      // Add some randomness to simulate ML model uncertainty
      val randomFactor = Random.nextGaussian() * 0.1
      
      Math.max(0.0, Math.min(1.0, baseScore + randomFactor))
    }
  }
  
  // -------------------------
  // Simple aggregation function for post-level toxicity
  // -------------------------
  def aggregatePostToxicity(comments: Iterable[(CommentToxicityEvent, Double)]): CommentToxicityModels.CommentToxicityNotification = {
    val commentsList = comments.toList
    val postId = commentsList.head._1.post_id
    val totalComments = commentsList.length
    val toxicComments = commentsList.count(_._2 > 0.5)
    val toxicityRatio = toxicComments.toDouble / totalComments
    val flagged = toxicityRatio > 0.3
    
    val timestamps = commentsList.map(_._1.event_timestamp)
    val windowStart = timestamps.min
    val windowEnd = timestamps.max
    
    CommentToxicityModels.CommentToxicityNotification(
      post_id = postId,
      window_start = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        .format(new java.util.Date(windowStart)),
      window_end = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        .format(new java.util.Date(windowEnd)),
      total_comments = totalComments,
      toxic_comments = toxicComments,
      toxicity_ratio = toxicityRatio,
      flagged = flagged
    )
  }
  
  // -------------------------
  // Main pipeline
  // -------------------------
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val opts = PipelineArgs.commentToxcityPipelineArgs(args)

    // Enable streaming
    sc.optionsAs[org.apache.beam.sdk.options.StreamingOptions].setStreaming(true)

    println("Starting Real-Time Comment Toxicity Pipeline...")
    println("Features:")
    println("  - Real-time comment ingestion from Pub/Sub")
    println("  - ML-based toxicity classification")
    println("  - Time-windowed aggregation at post level")
    println("  - Threshold-based flagging (>30% toxicity)")
    println("  - BigQuery output for dashboards & alerts")

    // -------------------------
    // STREAMING INPUT: Pub/Sub
    // -------------------------
    val commentStream = sc.read(PubsubIO.string(opts.inputSubscription))(PubsubIO.ReadParam(PubsubIO.Subscription))
      .flatMap(json => _root_.io.circe.parser.decode[CommentToxicityEvent](json).toOption)
      .withName("ParseCommentEvents")

    // -------------------------
    // TOXICITY CLASSIFICATION
    // -------------------------
    val classifiedComments = commentStream
      .applyTransform(ParDo.of(new ToxicityClassifierDoFn()))
      .withName("ClassifyToxicity")

    // -------------------------
    // WINDOWING & AGGREGATION
    // -------------------------
    val windowedComments = classifiedComments
      .keyBy(_._1.post_id)
      .applyTransform(Window.into(FixedWindows.of(Duration.standardMinutes(5))))
      .withName("WindowByPost")

    // -------------------------
    // POST-LEVEL AGGREGATION
    // -------------------------
    val postAggregations = windowedComments
      .groupByKey
      .map { case (postId, comments) => aggregatePostToxicity(comments) }
      .withName("AggregatePostToxicity")

    // -------------------------
    // FILTER FLAGGED POSTS
    // -------------------------
    val flaggedPosts = postAggregations
      .filter(_.flagged)
      .withName("FilterFlaggedPosts")

    // -------------------------
    // OUTPUT TO BIGQUERY
    // -------------------------
    flaggedPosts
      .map(BigQueryType[CommentToxicityModels.CommentToxicityNotification].toTableRow)
      .saveAsCustomOutput(
        "toxicity-notifications-bq",
        BigQueryIO
          .writeTableRows()
          .to(opts.outputBQTable)
          .withSchema(BigQueryType[CommentToxicityModels.CommentToxicityNotification].schema)
          .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(WriteDisposition.WRITE_APPEND)
      )

    // -------------------------
    // DLQ FOR ERROR HANDLING (Optional)
    // -------------------------
    // DLQ handling can be added here if needed

    // -------------------------
    // RUN PIPELINE
    // -------------------------
    val result = sc.run()
    result.waitUntilFinish()
    
    println("Comment Toxicity Pipeline completed successfully!")
  }
}
