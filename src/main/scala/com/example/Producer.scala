package com.example

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

import scalaz.concurrent._
import scalaz.stream._
import com.google.common.io.Resources
import java.util.Properties
import scalaz._, Scalaz._

object Producer {
  val producerConfigString = "producer.props"
  val numbers: Process[Task, Int] = Process.emitAll(0 until 500000).toSource

  def safeMain: Task[Unit] = {
    createProducer(producerConfigString).flatMap(x => produce(x).run)
  }

  def createProducer(producerConfigString: String): Task[KafkaProducer[String, String]] = Task {
    val properties = new Properties()
    val props = Resources.getResource(producerConfigString).openStream()
    properties.load(props)
    new KafkaProducer(properties)
  }

  def produce(producer: KafkaProducer[String, String]): Process[Task, Unit] = numbers.flatMap { i =>
    val message = createMessage("test", System.nanoTime(), i)
    val producerRecord = new ProducerRecord[String, String]("fast-messages", message)
    val task = sendRecord(producer, producerRecord) >> createMarkerTasks(producer, i)
    Process.eval_(task)
  }.onComplete(closeProducer(producer))

  def createMarkerTasks(producer: KafkaProducer[String, String], i: Int): Task[Unit] = {
    if (i % 1000 != 0) Task.point(())
    else {
      createAndSendMessage(producer, i, "fast-messages", "marker") >>
        createAndSendMessage(producer, i, "summary-markers", "other") >>
        logSentMessage(i)
    }
  }

  def logSentMessage(i: Int): Task[Unit] = Task {
    println("Sent msg number " + i)
  }

  def createAndSendMessage(producer: KafkaProducer[String, String], i: Int, topic: String, `type`: String): Task[Unit] = Task {
    val message = createMessage(`type`, System.nanoTime(), i)
    val producerRecord = new ProducerRecord[String, String](topic, message)
    println(producerRecord)
    sendRecord(producer, producerRecord)
  }

  def sendRecord(producer: KafkaProducer[String, String], producerRecord: ProducerRecord[String, String]): Task[Unit] = Task {
    producer.send(producerRecord)
  }

  def closeProducer(producer: KafkaProducer[String, String]) = Process.eval_(Task {
    producer.close()
  })

  def flushProducer(producer: KafkaProducer[String, String]): Task[Unit] = Task {
    producer.flush()
  }

  def createMessage(`type`: String, nanoTime: Double, i: Int): String = {
    s"""{"type":"${`type`}", "t":${nanoTime * 1e-9}, "k":${i}}"""
  }
}

