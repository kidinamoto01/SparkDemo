package sheshou

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by b on 17/4/6.
  */

object SparkToJDBC {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBCrExample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // val sqlContext = new SQLContext(sc)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    val data = sc.parallelize(List(("iteblog", 30), ("iteblog", 29), ("com", 40), ("bt", 33), ("www", 23)))

    import sqlContext.implicits._

    val df = data.toDF.toDF("id","val")
    val dfWriter = df.write.mode("append").option("driver", "com.mysql.jdbc.Driver")
    dfWriter.jdbc("jdbc:mysql://localhost:3306/test", "my_new_table", prop) //df.createJDBCTable(url, "sparktomysql", false)
  }
}
