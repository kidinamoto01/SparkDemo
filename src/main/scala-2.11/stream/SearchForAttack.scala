package stream

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by b on 17/4/1.
  */
object SearchForAttack {

  case class RawDataRecord(id:String,category: String, ip1: String,ip2:String)
  case class ResultRecord(time:String,category:String ,ip1:String,ip2:String)
  /*
  * 判断连续三次是否相等
  * */
  def compareData(st: Array[RawDataRecord]): Int ={
    var result = 0

    if(st.length == 3){
      //类别相同
      if(st(0).category==st(1).category==st(2).category){
        if(st(0).category=="A"){
          //ip 相同
          if(st(0).ip1==st(1).ip1==st(2).ip1){

            if(st(0).ip2==st(1).ip2==st(2).ip2){
              result = 1
            }
          }
        }
      }



      return result
    }


    else
      return 0

  }
  def saveResultToFile():Unit={

  }

  def main(args : Array[String]) {

    val conf = new SparkConf().setAppName("FilterExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val compareRDD = ArrayBuffer[RawDataRecord]()
    val srcRDD = sc.textFile("/Users/b/Documents/andlinks/sougou-train/new.txt").filter(_.nonEmpty).map {
      x =>
        val data = x.trim.split(",")
        compareRDD+=RawDataRecord(data(0), data(1),data(2),data(3))
        compareRDD
        compareData(compareRDD.toArray)
    }
  }

}
