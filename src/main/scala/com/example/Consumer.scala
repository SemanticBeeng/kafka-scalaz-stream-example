package com.example

import java.io.InputStream
import java.util.Properties

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.google.common.io.Resources
import org.HdrHistogram.Histogram
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

import scala.collection.JavaConverters.{iterableAsScalaIterableConverter, seqAsJavaListConverter}
import scalaz.concurrent.Task
import scalaz.stream._
import scalaz._
import Scalaz._

object Consumer {
  val initialTimeouts = 0
  val incTimeouts = State.modify[Int](_ + 1)
  val resetTimeouts = State.modify[Int](_ => initialTimeouts)

  def printTimeouts(recordsSize: Int)(timeouts: Int): Task[(Int, Unit)] = Task.now {
    println(s"Got ${recordsSize} records after ${timeouts} timeouts\n")
    (timeouts, ())
  }

  def safeMain: Task[Unit] = {
    val stats: Histogram = new Histogram(1, 10000000, 2)
    val global: Histogram = new Histogram(1, 10000000, 2)
    val configString: String = "consumer.props"
    createConsumerTask(stats = stats, global = global, configString = configString)
  }

  def configureConsumer(resourceString: String): Task[KafkaConsumer[String, String]] = Task {
    val properties: Properties = new Properties()
    val props: InputStream = Resources.getResource(resourceString).openStream()
    properties.load(props)
    new KafkaConsumer[String, String](properties)
  }

  def handleConsumerRecord(consumerRecord: ConsumerRecord[String, String], stats: Histogram, global: Histogram): Task[Unit] = {
    consumerRecord.topic match {
      case "fast-messages" =>
        val json: JsonNode = parseJson(consumerRecord.value)
        getMessageType(json) match {
          case "test" =>
            handleFastMessagesTest(stats, global, getMessageTime(json))
          case "marker" =>
            handleFastMessagesMarker(stats, global)
        }
      case "summary-markers" =>
        handleSummaryMarkers
      case _ =>
        throw new IllegalStateException("Shouldn't be possible to get message on topic " + consumerRecord.topic)

    }

  }

  def handleFastMessagesMarker(stats: Histogram, global: Histogram): Task[Unit] = Task {
    println(statsString(stats))
    println(statsString(global))
    stats.reset()
  }

  def handleFastMessagesTest(stats: Histogram, global: Histogram, messageTime: Double): Task[Unit] = Task {
    val latency: Long = ((System.nanoTime() * 1e-9 - messageTime) * 1000).toLong
    stats.recordValue(latency)
    global.recordValue(latency)
  }

  def handleSummaryMarkers: Task[Unit] = Task {
    println("ignoring message to summary markers")
  }

  def poll(subscribedConsumer: KafkaConsumer[String, String], timeoutMs: Long): Task[List[ConsumerRecord[String, String]]] = Task {
    subscribedConsumer.poll(timeoutMs).asScala.toList
  }

  def continuallyPoll(subscribedConsumer: KafkaConsumer[String, String], stats: Histogram, global: Histogram): Task[Unit] = {

    val task: TaskState[Unit] = liftTask(poll(subscribedConsumer, 100)).flatMap(x => handleRecords(x, stats, global))

    Process.repeatEval(task.run(initialTimeouts)).run
  }

  def handleRecords(recordsList: List[ConsumerRecord[String, String]], stats: Histogram, global: Histogram): TaskState[Unit] = {
    if (recordsList.isEmpty) liftState(incTimeouts)
    else for {
      timeouts <- liftState(get[Int])
      _ <- StateT[Task, Int, Unit](printTimeouts(recordsList.size))
      _ <- liftState(resetTimeouts)
      result <- liftTask(recordsList.traverse_[Task](handleConsumerRecord(_, stats, global)))
    } yield result

  }

  def createConsumerTask(stats: Histogram, global: Histogram, configString: String): Task[Unit] = for {
    c <- configureConsumer(configString)
    sc <- subscribeConsumer(c, List("fast-messages", "summary-markers"))
    result <- continuallyPoll(sc, stats, global)
  } yield result

  def statsString(stats: Histogram): String = {
    s"${stats.getTotalCount} messages received in period, latency(min, max, avg, 99%%) = ${stats.getValueAtPercentile(0)}, " +
      s"${stats.getValueAtPercentile(100)}, ${stats.getMean}, ${stats.getValueAtPercentile(99)} (ms)\n"
  }

  def parseJson(value: String): JsonNode = new ObjectMapper().readTree(value)

  def getMessageType(json: JsonNode): String = json.get("type").asText

  def getMessageTime(json: JsonNode): Double = json.get("t").asDouble

  def parseMessageType(value: String): String = getMessageType(parseJson(value))

  def subscribeConsumer(consumer: KafkaConsumer[String, String], topics: List[String]): Task[KafkaConsumer[String, String]] = Task {
    consumer.subscribe(topics.asJava)
    consumer
  }

}
