package segment

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row

/**
  * Created by b on 17/3/3.
  */
object BaysienOffline {


  case class RawDataRecord(category: String, text: String)
  case class Sample(s0: String, s1: String)

  def delayed( t: => String ) = {
    println( t)
    val temp = ToAnalysis.parse(t)

    //加入停用词
   // FilterModifWord.insertStopWords(util.Arrays.asList("r", "n"))
    //加入停用词性
   // FilterModifWord.insertStopNatures("w", null, "ns", "r", "u", "e")
   // val filter = FilterModifWord.modifResult(temp)
    //此步骤将会只取分词，不附带词性
 //   val word = for (i <- Range(0, filter.size())) yield filter.get(i).getName
   // word.mkString("\t")

 //   println( word)

    temp.toString("\t")
  }

  def main(args : Array[String]) {

    val conf = new SparkConf().setAppName("BayTest")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //test
    val srcRDD = sc.textFile("/home/suyu/Documents/sms/anjs_results.txt").map(_.replace('(',' ')).map(_.replace(')',' ')).map {
      x =>
        val data = x.split(",")
        RawDataRecord(data(0),data(1))
    }


    //val splits = srcRDD.randomSplit(Array(1.0, 0.0))
    val trainingDF = srcRDD.toDF()
    //将词语转换成数组
    val tokenize = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenize.transform(trainingDF)
    // println("output1：")
    //wordsData.select($"category",$"text",$"words").take(1).foreach(println)

    //计算每个词在文档中的词频
    val hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
    //load the model
    val oldModel = NaiveBayesModel.load(sc, "/home/suyu/Documents/sms/myNaiveBayesModel")

    val record = "100,【汇付天下】尊敬的用户，你的结算银行卡变更申请信息审批已通过，请查收。"
    val data = record.split(",")
    val id = data(0)
    val tmp = delayed(data(1)).toString()
    val b = Array(Array(id,tmp))

    val rdd = sc.makeRDD(b)
    //create RDD
    val testDF = rdd.map {
      case Array(s0, s1) => RawDataRecord(s0, s1) }.toDF()
    println("output1：")
    testDF.select($"category",$"text").take(1).foreach(println)
    //create tokenizer
    val newDF =trainingDF.unionAll(testDF)
    println("cols："+ newDF.count())
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    // get the transformed data
    val testwordsData = tokenizer.transform(newDF)
    println("output2：")
    //hashing
    testwordsData.select($"category",$"text",$"words").take(1).foreach(println)
    val featurizedData = hashingTF.transform(testwordsData)
    //println("output 2：")
    // featurizedData.select($"category",$"text",$"words",$"rawFeatures").take(1).foreach(println)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val testrescaledData = idfModel.transform(featurizedData)
    //println("output3：")
    //testrescaledData.select($"category", $"features").take(1)
    val newTest = testrescaledData.filter(testrescaledData("category")>10).show()

    val newTests =  testrescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
    /*val testDataRdd =  testrescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }*/
    //testDataRstdd.foreach(p=>println(p.features))
    println("test1 : "+newTests.count())

    /* println("count: " + srcRDD.count())*/
    val results =newTests.foreach{ p =>
      val predictLabel = oldModel.predict(p.features)
      if (p.label == 100.0 ) println("predict : " + predictLabel+p.label)
    }

    /*
       val resultRDD = newTests.map{ p=>
         val r = oldModel.predict(p.features)
         p.label.toString+','+r

       }*/

  }
}
