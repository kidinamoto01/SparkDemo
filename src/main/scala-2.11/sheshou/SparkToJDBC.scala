package sheshou

import java.sql.{DriverManager, ResultSet}
import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.MutableList

/**
  * Created by b on 17/4/6.
  */

object SparkToJDBC {

  case class Person(key:Int,fname:String,lname:String,address:String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBCrExample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    // val sqlContext = new SQLContext(sc)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    //get data from hive
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

    import sqlContext.implicits._

    //val df = data.toDF.toDF("id","val")
    val df = fetchedRes.toDF

    val dfWriter = df.write.mode("overwrite").option("driver", "com.mysql.jdbc.Driver")
    dfWriter.jdbc("jdbc:mysql://localhost:3306/test", "people", prop)

  }
}
