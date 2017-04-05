package sheshou

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import scala.collection.mutable.ArrayBuffer


/**
  * Created by b on 17/4/5.
  */
object PredictNumber {
  //原始数据
  //case class RawDataRecord(id:String,category: String, ip1: String,ip2:String)
  case class RawDataRecord(id:String,month: String, num: Int)
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

    val conf = new SparkConf().setAppName("FilterExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val compareRDD = ArrayBuffer[RawDataRecord]()
    val srcRDD = sc.textFile("/Users/b/Documents/andlinks/predict.txt").filter(_.nonEmpty)
    val resultRDD = srcRDD.coalesce(1,false).map {
      x =>
        val data = x.trim.split(",")
        compareRDD.append(RawDataRecord(data(0), data(1),data(2).toInt))
        val input = compareRDD
        val result = compareInputs(input.toArray)

        result
    }


    //save to file

    resultRDD.toDF().coalesce(1).write
      .format("com.databricks.spark.csv")
      .mode("append")
      .option("header", "true")
      .save("/Users/b/Documents/andlinks/sougou-train/result")
    //.write.mode(SaveMode.Append).text("/Users/b/Documents/andlinks/sougou-train/")
  }
}
