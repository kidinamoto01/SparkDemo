package sheshou

import java.util

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by suyu on 12/11/16.
  */
object SparkStreamingWindows {


  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |  <spliter_in> is the spliter for input
                            |  <spliter_out> is the spliter for output
                            |  <m_length> is the designed length of the message
        """.stripMargin)
      System.exit(1)
    }


    val Array(brokers, topics,spliter_in,spliter_out,m_length) = args
    println(brokers)
    println(topics)
    println(spliter_in)
    println(spliter_out)
    println(m_length)
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaDataCleaning").setMaster("local[*]")
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet).map(_._2)
    //val sc = new SparkContext(sparkConf)
  //  val sqlContext = new SQLContext(sc)
    // Get the lines, split them into words, count the words and print
    messages.foreachRDD{
      rdd=>rdd.foreachPartition { partitionOfRecords =>
        //create a producer for each partition
        val props = new util.HashMap[String, Object]()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
          "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String,String](props)
        partitionOfRecords.foreach { record =>
          if (record.length > 0) {
            println(spliter_in)
            val s:Char = spliter_in.charAt(0)
            val data:Array[String] = record.split(s)

            //get the count of columns
            val count =  data.size
            println(s)
            println(count)
            if(count == m_length.toInt ){
              //get context
              val newcontent = (record.replace(spliter_in,spliter_out)).toString()
              val message = new ProducerRecord[String, String]("cleaned_output", null, newcontent)
              producer.send(message)
            }
          }

        }
      }
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
