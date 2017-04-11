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
    println(file.first())
    file.coalesce(1).write
      .format("com.databricks.spark.csv")
      .mode("append")
      .option("delimiter","\t")
      .save("/Users/b/Documents/sheshou")


  }
}
