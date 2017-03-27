package stream

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by b on 17/3/15.
  *
  */
object StreamingMachineLearning {

  case class RawDataRecord(category: String, text: String)

  def getDataFrame(field1:String,field2:String,sc:SparkContext): Unit ={

    val tmp = Array(Array(field1,field2))
    val rdd = sc.makeRDD(tmp)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val testDF = rdd.map {
      case Array(s0, s1) => RawDataRecord(s0, s1) }.toDF()
    testDF
  }

  def loadDataFromKafka(topics: String,
                        brokerList: String,
                        ssc: StreamingContext): DStream[String] = {
  val topicsSet = topics.split(",").toSet
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList)
  val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaParams, topicsSet)
    messages.map(_._2)
  }

    /*def transformInput(inputData:DataFrame): RDD[LabeledPoint] ={


    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(inputData)
    val hashingTF = new HashingTF().setNumFeatures(500).setInputCol("words").setOutputCol("rawFeatures")
    val featurizedData = hashingTF.transform(wordsData)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    //转换成Bayes的输入格式
    val trainDataRdd = rescaledData.select("category","features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }.rdd
    trainDataRdd
  }*/

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
    val ssc = new StreamingContext(sc, Seconds(5))

    //val kuduContext = new KuduContext(ssc.sparkContext, kuduHost)
    // val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val brokerList = kafkaHost
    val topics = "test"
    val current_time = System.currentTimeMillis

    val numFeatures = 2
    val model = NaiveBayesModel.load(sc, "/Users/b/Documents/andlinks/model")
    //test

    val dstream = loadDataFromKafka(topics, brokerList, ssc)
    val stream = dstream.map { x =>
      val data = x.split(",")

      val result =model.predict(TrainingUtils.featureVectorization(data(1)))
//model.predict(TrainingUtils.featureVectorization(x._2)))
      //case Row(label: String, features: Vector) =>
      //LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
      data(0)+" "+result
    }.print()
    ssc.start()
    ssc.awaitTermination()
  }
}