package segment

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row

/**
  * Created by suyu on 17/3/3.
  */
object CalculateModel {

  case class RawDataRecord(category: String, text: String)

  def main(args : Array[String]) {

    val conf = new SparkConf().setAppName("BayesExample")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val srcRDD = sc.textFile("/Users/b/Documents/andlinks/answer/file.txt").map {
      x =>
        val data = x.trim().split("|")
        RawDataRecord(data(0),data(1))
    }

    //70%作为训练数据，30%作为测试数据
    val splits = srcRDD.randomSplit(Array(0.7, 0.3))
    val trainingDF = splits(0).toDF()
    val testDF = splits(1).toDF()

    //将词语转换成数组
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(trainingDF)
    println("output1：")
    wordsData.select($"category",$"text",$"words").take(1).foreach(println)

    //计算每个词在文档中的词频
    val hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
    val featurizedData = hashingTF.transform(wordsData)
    println("output2：")
    featurizedData.select($"category", $"words", $"rawFeatures").take(1)


    //计算每个词的TF-IDF
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    println("output3：")
    rescaledData.select($"category", $"features").take(1)

    //转换成Bayes的输入格式
    val trainDataRdd = rescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }.rdd
    println("output4：")
    trainDataRdd.take(1)

    //训练模型
    val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")

    //测试数据集，做同样的特征表示及格式转换
    val testwordsData = tokenizer.transform(testDF)
    val testfeaturizedData = hashingTF.transform(testwordsData)
    val testrescaledData = idfModel.transform(testfeaturizedData)
    val testDataRdd = testrescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    //对测试数据集使用训练模型进行分类预测
    val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))
    //testpredictionAndLabel.foreach(println)
    //统计分类准确率
    val testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
    println("output5：")
    println(testaccuracy)

  }


}
