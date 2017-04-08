package sheshou

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.MutableList


/**
  * Created by b on 17/4/5.
  */
object PredictNumber {
  //原始数据
  //case class RawDataRecord(id:String,category: String, ip1: String,ip2:String)
  case class RawDataRecord(id:String,month: String, num: Int)
  case class Person(key:Int,fname:String,lname:String,address:String)
  //结果存储结构
  case class ResultRecord(id:String,month: String, num: Int,increase:Double,next:Int)
  /**
    * Created by b on 17/4/5.
    * @define 输入原数据
    *
    */
  def compareInputs(input: Array[RawDataRecord]): ResultRecord = {
    var result:ResultRecord =ResultRecord("","",0,0,0)

    //初始化变量
    var id = ""
    var month = ""
    var num =0
    var next = 0
    var increase:Double = 0

    //打印变量长度
    println("****"+input.length)

    if(input.length >= 2){
      val st = input.takeRight(2)
      //println(st(0).category+st(1).category+st(2).category)
      //有效数据

      if((st(0).num!=0)&&(st(1).num!= 0)){

        //月份
        month = st(1).month
        //id
        id = st(1).id
          //计算增长率
        increase = (st(1).num-st(0).num).toDouble/st(0).num.toDouble

        num = st(1).num
        //预测下一个
        next = (st(1).num.toDouble *(1.0+increase)).toInt
      }

    }

    return ResultRecord(id,month,num,increase,next)

  }

  def main(args : Array[String]) {
    if (args.length < 3) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }

    //get arguments
    val url = args.mkString.split(' ').take(0)
    val user = args.mkString.split(' ').take(1)
    val password = args.mkString.split(' ').take(2)


    val conf = new SparkConf().setAppName("FilterExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    //create hive context
    //val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    Class.forName("org.apache.hive.jdbc.HiveDriver");
    val conn = DriverManager.getConnection("jdbc:hive2://192.168.1.23:10000/default", "admin", "123456")

    //get input table
    val res: ResultSet = conn.createStatement
      .executeQuery("SELECT * FROM person_hbase")

    //fetch all the data
    val fetchedRes = MutableList[Person]()

    while(res.next()) {
      var rec = Person(
        res.getInt("key"),
        res.getString("fname"),
        res.getString("lname"),
        res.getString("address"))
      fetchedRes += rec
    }



    //save data
    fetchedRes.foreach(println)
    val resultRDD = fetchedRes.map{ x =>

      x.fname
    }

    import sqlContext.implicits._
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    resultRDD.toDF().registerTempTable("temp")
    //sqlContext.sql("CREATE TABLE IF NOT EXISTS mytable as select * from  temp")
    //sqlContext.sql("CREATE TABLE IF NOT EXISTS mytable as select * from  temp")
    //saveAsTable("results_test_hive")
    //resultRDD.toDF().write.format("parquet").mode("append").saveAsTable("results_test_hive")

    println(sqlContext.sql("select * from mytable").count())

    //close connection
    conn.close()

    /* val srcRDD = sc.textFile("/Users/b/Documents/andlinks/predict.txt").filter(_.nonEmpty)
     val resultRDD = srcRDD.coalesce(1,false).map {
       x =>
         val data = x.trim.split(",")
         compareRDD.append(RawDataRecord(data(0), data(1),data(2).toInt))
         val input = compareRDD
         val result = compareInputs(input.toArray)

         result
     }*/



    /*val resultRDD = fetchedRes.map {
      x =>
        //val data = x..split(",")
        //compareRDD.append(RawDataRecord(data(0), data(1),data(2).toInt))
        compareRDD.append(x)
        //val input = compareRDD
       // val result = compareInputs(compareRDD.toArray)

       // result
        x.key
    }*/

    //save to file
   /* import sqlContext.implicits._

    resultRDD.toDF().coalesce(1).write
      .format("com.databricks.spark.csv")
      .mode("append")
      .option("header", "true")
      .save("/Users/b/Documents/andlinks/sougou-train/result")
*/
    //save to table
  }
}
