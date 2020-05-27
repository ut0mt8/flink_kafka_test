package com.futomaki

import java.util.Properties
import io.circe.parser
import io.circe.generic.auto._
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
//import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer,FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.{SerializationSchema,SimpleStringSchema}

object FlinkKafkaTest {

  val logger: Logger = LoggerFactory.getLogger("FlinkLogger")

  case class Event(id: Int, value: String)
  case class EventWithSize(event: Event, size: Int)
  case class EventStat(id: Int, size: Int, count: Int, events: List[Event])

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaProperties = new Properties

    kafkaProperties.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProperties.setProperty("group.id", "flink-group")
    val kafkaConsumer = new FlinkKafkaConsumer(
      "flink-in",
      new SimpleStringSchema,
      kafkaProperties
    )

    val kafkaProducer = new FlinkKafkaProducer(
      "flink-out",
      eventStatSchema,
      kafkaProperties
    )

    val kafkaStream: DataStream[String] = env.addSource(kafkaConsumer).name("kafka-in")

    val eventStream: DataStream[Event] = kafkaStream
      .map(json => {
        val jsonResult = parser.decode[Event](json)
        val event: Event = jsonResult match {
          case Right(ev) => Event(ev.id, ev.value)
          case Left(er) => Event(-1, "")
        }
        event
      }).filter(_.id >= 0)

    val keyedEventStream: KeyedStream[Event, Int] = eventStream.keyBy(e => e.id)

    val filteredEventStream: DataStream[EventWithSize] = keyedEventStream
      .mapWithState((event: Event, size: Option[Int]) => {
        val newsize: Int = event.toString.getBytes.length
        val es: Tuple2[EventWithSize, Some[Int]] = size match {
          case Some(c) => (EventWithSize(event, c+newsize), Some(c+newsize))
          case None => (EventWithSize(event, newsize), Some(newsize))
        }
        es
      }).filter(ews => {
        if (ews.size > 100000000) {
          logger.info(s"Too big: id ${ews.event.id}, size: ${ews.size}")
          false
        } else {
          true
        }
      })

    val keyedFilteredEventStream = filteredEventStream.keyBy(e => e.event.id)

    //case class EventStat(id: Int, size: Int, count: Int, events: List(EventWithSize))
    val eventStatStream = keyedFilteredEventStream
      //.window(EventTimeSessionWindows.withGap(Time.minutes(2)))
      .window(TumblingProcessingTimeWindows.of(Time.minutes(60)))
      .fold(EventStat(0,0,0,List()))( (acc, occ) => EventStat(occ.event.id, acc.size+occ.size, acc.count+1, occ.event :: acc.events) )

    eventStatStream.addSink(kafkaProducer).name("kafka-out")

    env.execute("KafkaExample")
  }

  val eventStatSchema: SerializationSchema[EventStat] = new SerializationSchema[EventStat]() {
    override def serialize(es: EventStat): Array[Byte] = {
      s"id: ${es.id} size: ${es.size} count: ${es.count}".getBytes
    }
  }

}

