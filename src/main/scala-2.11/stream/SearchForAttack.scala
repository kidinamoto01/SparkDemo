package stream

import java.sql.DriverManager

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
  def compareData(input: Array[RawDataRecord]): Int ={
    var result = 0
    println("****"+input.length)
    if(input.length >= 3){
      val st = input.takeRight(3)
      println(st(0).category+st(1).category+st(2).category)
      //类别相同
      if((st(0).category==st(1).category)&&(st(1).category==st(2).category)){
       // if(st(0).category=='A'){
          //ip1 相同
          if((st(0).ip1==st(1).ip1)&&(st(1).ip1==st(2).ip1)){
            //ip2相同
            if((st(0).ip2==st(1).ip2)&&(st(1).ip2==st(2).ip2)){
              result = 1
            }
          }
       // }
      }




      return result
    }


    else
      return 0

  }

  def compareInputs(input: Array[RawDataRecord],targets:String): Int ={
    var result = 0

    println("****"+input.length)
    if(input.length >= 3){
      val st = input.takeRight(3)
      println(st(0).category+st(1).category+st(2).category)
      //类别相同
      if((st(0).category==st(1).category)&&(st(1).category==st(2).category)){
        if(st(0).category.equals(targets)){
        //ip1 相同
        if((st(0).ip1==st(1).ip1)&&(st(1).ip1==st(2).ip1)){
          //ip2相同
          if((st(0).ip2==st(1).ip2)&&(st(1).ip2==st(2).ip2)){
            result = 1
          }
        }
        }
      }

    }

    return result

  }

  def main(args : Array[String]) {

    val conf = new SparkConf().setAppName("FilterExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val compareRDD = ArrayBuffer[RawDataRecord]()
    val srcRDD = sc.textFile("/Users/b/Documents/andlinks/sougou-train/ip.txt").filter(_.nonEmpty)
    val resultRDD = srcRDD.coalesce(1,false).map {
      x =>
        val data = x.trim.split(",")
        compareRDD.append(RawDataRecord(data(0), data(1),data(2),data(3)))
        val input = compareRDD
        val result = compareInputs(input.toArray,"A")

        result.toString
    }


    val filteredRDD = resultRDD
    //save to file
    val username = "root"
    val password = "root"
    val url = "jdbc:mysql://localhost:3306/testuser=" + username + "&password=" + password;

  Class.forName("com.mysql.jdbc.Driver").newInstance

    val conn = DriverManager.getConnection(url,username,password)
   /* val del = conn.prepareStatement("INSERT INTO sample (value) VALUES (?)")
    val i = 1
    del.setLong (1, i)
    del.executeUpdate
    conn.close()*/

   // resultRDD.toDF().coalesce(1).write.format("com.databricks.spark.csv").mode("append").option("header", "true").save("/tmp/result.csv")

  //  resultRDD.saveAsTextFile("file///tmp/result.csv")
    //.write.mode(SaveMode.Append).text("/Users/b/Documents/andlinks/sougou-train/")
  }
/*import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

def merge(srcPath: String, dstPath: String): Unit =  {
   val hadoopConfig = new Configuration()
   val hdfs = FileSystem.get(hadoopConfig)
   FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig, null)
   // the "true" setting deletes the source files once they are merged into the new output
}


val newData = << create your dataframe >>


val outputfile = "/user/feeds/project/outputs/subject"
var filename = "myinsights"
var outputFileName = outputfile + "/temp_" + filename
var mergedFileName = outputfile + "/merged_" + filename
var mergeFindGlob  = outputFileName

    newData.write
        .format("com.databricks.spark.csv")
        .option("header", "false")
        .mode("overwrite")
        .save(outputFileName)
    merge(mergeFindGlob, mergedFileName )
    newData.unpersist()*/
}
