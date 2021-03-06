import java.util.Calendar

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by b on 17/3/3.
  */
object WordCount {

  def main(args: Array[String]) {
    val logFile = "/usr/local/share/spark-2.1.0-bin-hadoop2.6/README.md" // Should be some file on your system
    val filepath = "/Users/b/Documents/andlinks/sheshou/log/0401log3(1).txt"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //read json file
    val file =sqlContext.read.json(filepath).toDF()
    val cal = Calendar.getInstance()
    val date =cal.get(Calendar.DATE )
    val Year =cal.get(Calendar.YEAR )
    val Month1 =cal.get(Calendar.MONTH )
    val Month = Month1+1
    println(file.first())
    file.coalesce(1).write
      .format("com.databricks.spark.csv")
      .mode("append")
      .option("delimiter","\t")
      .save("hdfs://192.168.1.21:8020/tmp/"+date+Month+Year)


  }
}
