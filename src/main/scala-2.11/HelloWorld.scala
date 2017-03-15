/**
  * Created by b on 17/3/3.
  */
import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Test")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    println(sc)
  }
}
