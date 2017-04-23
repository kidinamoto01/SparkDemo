package sheshou


import java.util.{Calendar, Properties}

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by fenglu on 2017/4/20.
  */
object Readkafka {

  def main(args: Array[String]): Unit = {

    val Array(brokers,zks,url,user,passwd) = args

    println(brokers)
    println(zks)
    println(url)
    println(user)
    println(passwd)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("Shooter")
    //sparkConf.set("spark.hadoop.parquet.enable.summary-metadata", "true")
    //spark.hadoop.parquet.enable.summary-metadata false
    val  sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))


    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers.toString,
      "zookeeper.connect" -> zks.toString)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams,  Set("webmiddle")).map(_._2)

    val messages3 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("windowslogin")).map(_._2)

    val messgesnet = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("netstdsonline")).map(_._2)


    // Get the lines
    messages.foreachRDD{ x =>

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      // val text = sqlContext.read.json(x)
      val text = sqlContext.read.json(x)

      //get json schame
      text.toDF().printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if(text.count()>0)
      {

        println("write")
        //text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/tmp/sheshou/parquet/")
        //Get Date
        val cal = Calendar.getInstance()
        val date = cal.get(Calendar.DATE)
        val Year = cal.get(Calendar.YEAR)
        val Month1 = cal.get(Calendar.MONTH)
        val Month = Month1+1
        val Hour = cal.get(Calendar.HOUR_OF_DAY)

        text.write.format("parquet").mode(SaveMode.Append).parquet("/sheshou/data/parquet/"+"webmiddle"+"/"+Year+"/"+Month+"/"+date+"/"+Hour+"/")
      }

    }

    // Get the lines
    messages3.foreachRDD{ x =>

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      // val text = sqlContext.read.json(x)
      val text = sqlContext.read.json(x)

      //get json schame
      text.toDF().printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if(text.count()>0)
      {

        println("write3")
        //text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/tmp/sheshou/parquet/")
        //Get Date
        val cal = Calendar.getInstance()
        val date = cal.get(Calendar.DATE)
        val Year = cal.get(Calendar.YEAR)
        val Month1 = cal.get(Calendar.MONTH)
        val Month = Month1+1
        val Hour = cal.get(Calendar.HOUR_OF_DAY)

        text.write.format("parquet").mode(SaveMode.Append).parquet("/sheshou/data/parquet/"+"windowslogin"+"/"+Year+"/"+Month+"/"+date+"/"+Hour+"/")
      }

    }

    messgesnet.foreachRDD { x =>
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      // val text = sqlContext.read.json(x)
      val text = sqlContext.read.json(x)

      //get json schame
      text.toDF().printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if (text.count() > 0) {

        //text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/tmp/sheshou/parquet/")
        //Get Date
        val cal = Calendar.getInstance()
        val date = cal.get(Calendar.DATE)
        val Year = cal.get(Calendar.YEAR)
        val Month1 = cal.get(Calendar.MONTH)
        val Month = Month1+1
        val Hour = cal.get(Calendar.HOUR_OF_DAY)

        text.write.format("parquet").mode(SaveMode.Append).parquet("/sheshou/data/parquet/realtime/break" + "/" + Year + "/" + Month + "/" + date + "/" + Hour + "/")

        //get MySQL Dataframe
        val mysqlDF= sqlContext.sql("select  time as attack_time,  dstIP as dst_ip,srcip as src_ip, \"netstds\" as attack_type, \"0\" as src_country_code, srcLocation as src_country, srcLocation as src_city,\"0\" as dst_country_code,dstLocation as dst_country,dstLocation as dst_city,srclatitude as src_latitude, srclongitude as  src_longitude, dstLatitude as dst_latitude,dstLongitude as dst_longitude, time as  end_time, \"0\" as asset_id,toolName as assent_name,\"0\" as alert_level  from netstds ")

        val prop = new Properties()
        prop.setProperty("user", user)
        prop.setProperty("password", passwd)


        val dfWriter = text.write.mode("append").option("driver", "com.mysql.jdbc.Driver")
        text.registerTempTable("netstds")

        dfWriter.jdbc(url, "attack_list", prop)

      }
    }


    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
