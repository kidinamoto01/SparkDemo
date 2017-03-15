package stream

import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
/**
  * Created by b on 17/3/15.
  *
  */
object StreamingMachineLearning {
  /*def loadDataFromKafka(topics: String,
                        brokerList: String,
                        ssc: StreamingContext): DStream[String] = {

  }

  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Required arguments: <kudu host> <kafka host>")
      sys.exit(42)
    }
    val Array(kuduHost, kafkaHost) = args
    val conf = new SparkConf().setAppName("kudu meetup")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val ssc = new StreamingContext(sc, Seconds(10))
   // val kuduContext = new KuduContext(ssc.sparkContext, kuduHost)
    val brokerList = kafkaHost.split(",").toSet
    val topics = "meetupstream"
c
   /* val stream = loadDataFromKafka(topics, brokerList, ssc).transform { rdd =>
      val parsed = sqlContext.read.json(rdd)
      parsed.printSchema()
      parsed.rdd

    }*/


    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //val kafkaParams = Map[String, Object]("metadata.broker.list" -> brokerList)
    /* val stream = KafkaUtils.createDirectStream[String, String](
       StreamingContext,
       PreferConsistent,
       Subscribe[String, String](topicsSet, kafkaParams)
     )*/
    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    val stream = KafkaUtils.createDirectStream[String, String](
      StreamingContext,
      LocationStrategies.PreferConsistent,
      consumerStrategy
    )

    stream.map(_._2)
  }*/
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()

    val Array(brokers, topics) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    wordCounts.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}