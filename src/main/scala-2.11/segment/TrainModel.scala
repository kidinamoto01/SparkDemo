package segment

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by b on 17/3/14.
  * 使用SVM模型做二元判断
  * 初始变量有1000个，然后使用PCA减半
  */
object TrainModel {

  case class RawDataRecord(category: String, text: String)

  def main(args : Array[String]) {

    val conf = new SparkConf().setAppName("BayesExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sqlContext = new  SQLContext(sc)
    import sqlContext.implicits._
    /*val srcRDD = sc.textFile("/Users/b/Documents/andlinks/answer/file.csv").filter(_.nonEmpty).map{
      x =>
        val data = x.trim.split(",")
        RawDataRecord(data(0), data(1))
    }*/
    //读取已经分词的数据
    val srcRDD = sc.textFile("/Users/b/Documents/andlinks/sougou-train/new.txt").filter(_.nonEmpty).map{
       x =>
         val data = x.trim.split(",")
           RawDataRecord(data(0), data(1))

    }
    println("output1："+srcRDD.count())

    //70%作为训练数据，30%作为测试数据
    val splits = srcRDD.randomSplit(Array(0.7, 0.3))
    val trainingDF = splits(0).toDF()
    val testDF = splits(1).toDF()

    //将词语转换成数组
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(trainingDF)
    println("output1：")
    wordsData.printSchema()
    wordsData.select($"category",$"words").take(1).foreach(println)

    //计算每个词在文档中的词频
    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol("words").setOutputCol("rawFeatures")
    val featurizedData = hashingTF.transform(wordsData)
    println("output2：")
    featurizedData.printSchema()
    featurizedData.select($"category",$"rawFeatures").take(1).foreach(println)
    //计算每个词的TF-IDF
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData).cache()
    println("output3：")
    rescaledData.printSchema()
    rescaledData.select($"category",$"features").take(1).foreach(println)

    //转换成Bayes的输入格式
    val trainDataRdd = rescaledData.select($"category",$"features").
      filter("features is not null").
      filter("category is not null").map {
      case Row(label: String, features: org.apache.spark.ml.linalg.Vector) =>
        org.apache.spark.mllib.regression.LabeledPoint(label.toDouble, org.apache.spark.mllib.linalg.Vectors.dense(features.toArray))
    }
    println("output4："+trainDataRdd.rdd.count())
    trainDataRdd.printSchema()
    trainDataRdd.take(1).foreach(println)

      //PCA half the element
        val num = trainDataRdd.first().features.size/2
    println("output5 :"+ num)
        val pca = new PCA(num).fit(trainDataRdd.rdd.map(_.features))
        val training_pca = trainDataRdd.map(p => p.copy(features = pca.transform(p.features))).rdd
        println("output6：")
    training_pca.take(1).foreach(println)

    // Run training algorithm to build the model
    //use svm to get the key
    //DataValidators: Classification labels should be 0 or 1.

    val numIterations = 100
    val svmmodel = SVMWithSGD.train(training_pca, numIterations)
    // 查看训练结果
    val newFeatures = svmmodel.predict(training_pca.map(x=>x.features.asInstanceOf[org.apache.spark.mllib.linalg.Vector]))
    println("output7：")
    newFeatures.take(1).foreach(println)
/*
    //训练模型
    val model = NaiveBayes.train(training_pca, lambda = 1.0, modelType = "multinomial")
//    val model = NaiveBayes.train(trainDataRdd.rdd, lambda = 1.0, modelType = "multinomial")

*/
    //测试数据集，做同样的特征表示及格式转换
    val testwordsData = tokenizer.transform(testDF)
    val testfeaturizedData = hashingTF.transform(testwordsData)
    val testrescaledData = idfModel.transform(testfeaturizedData)
    val testDataRdd = testrescaledData.select($"category",$"features").map {
      case Row(label: String, features: org.apache.spark.ml.linalg.Vector) =>
        org.apache.spark.mllib.regression.LabeledPoint(label.toDouble, org.apache.spark.mllib.linalg.Vectors.dense(features.toArray))
    }
     val test_pca = testDataRdd.map(p => p.copy(features = pca.transform(p.features)))

    //对测试数据集使用训练模型进行分类预测
    val testpredictionAndLabel = test_pca.map(p => (svmmodel.predict(p.features), p.label))

    //打印预测结果
    //testpredictionAndLabel.rdd.foreach(println)
    //统计分类准确率
    val testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
    println("output8：")
    println(testaccuracy)
  }

}
