package sheshou

import java.util

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by b on 17/3/31.
  */
object SearchForBreach {

  case class RawDataRecord(category: String, ip1: String,ip2:String)

  def loadDataFromKafka(topics: String,
                        brokerList: String,
                        ssc: StreamingContext): DStream[String] = {
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    messages.map(_._2)
  }

  def compareData(st: Array[String]): Int ={
    val result = 0

    if(st.length == 3)

      return result
    else
      return 0

  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    if (args.length != 1) {
      println("Required arguments: <broker>")
      sys.exit(42)
    }
    val Array( kafkaHost) = args
    val conf = new SparkConf().setAppName("MeetUpStream").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val ssc = new StreamingContext(sc, Seconds(10))

    val brokerList = kafkaHost
    val topics = "test"

    //settings for kafka producer
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String,String](props)


    var loops = 0
    var compareRDD = Array("")
    val dstream = loadDataFromKafka(topics, brokerList, ssc)
    val stream = dstream.map{x =>
      val data = x.split(",")
      compareRDD(loops)= x
      loops +=1
      loops = loops%3
      if(compareData(compareRDD)== 1){
        //output topic is output_topic
        val message=new ProducerRecord[String, String]("output_topic",null,"aaaaa")
        producer.send(message)
      }
    }.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
