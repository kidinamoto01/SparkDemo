package segment
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by b on 17/3/9.
  * 使用TF/IDF计算出特征向量
  * 然后使用Bayes计算出特征向量和类型之间的关系
  * 可以做多类别回归>2
  */
object Test {

  case class RawDataRecord(category: String, text: String)

  case class InputData(category: Double, text: org.apache.spark.mllib.linalg.Vector)

  def main(args : Array[String]) {

    val conf = new SparkConf().setAppName("BayesExample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sqlContext = new  SQLContext(sc)
    import sqlContext.implicits._
   val srcRDD = sc.textFile("/Users/b/Documents/andlinks/sougou-train/new.txt").filter(_.nonEmpty).map{
      x =>
        val data = x.trim.split(",")
          RawDataRecord(data(0), data(1))
   }
   /*val srcRDD = sc.textFile("/Users/b/Documents/andlinks/sougou-train/new.txt").filter(_.nonEmpty).map{
      x =>
        val data = x.trim.split(",")
          RawDataRecord(data(0), data(1))

   }*/
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
    rescaledData.cache()
    rescaledData.select($"category",$"features").take(1).foreach(println)
    //rescaledData.select("category","features").collect().take(2).foreach(println)
    //val labeled = tmp.map(row => LabeledPoint(row.getDouble(0), row(1).asInstanceOf[Vector]))

    //转换成Bayes的输入格式
    val trainDataRdd = rescaledData.select($"category",$"features").
      filter("features is not null").
      filter("category is not null").map {
      case Row(label: String, features: org.apache.spark.ml.linalg.Vector) =>
        org.apache.spark.mllib.regression.LabeledPoint(label.toDouble, org.apache.spark.mllib.linalg.Vectors.dense(features.toArray))
    }
    println("output4："+trainDataRdd.rdd.count())
      /*.map(
      row => LabeledPoint(
        row.getAs[Double]("category"),
        row.getAs[org.apache.spark.mllib.linalg.Vector]("features")
      )
    )


    trainDataRdd.printSchema()

    val transRDD = trainDataRdd.rdd.map{
      row => LabeledPoint(
        r,
        row.getAs[org.apache.spark.mllib.linalg.Vector]("text")
      )

    }*/
/*
   //PCA half the element
    val num = trainDataRdd.first().features.size/2
    val pca = new PCA(num).fit(trainDataRdd.map(_.features))
    val training_pca = trainDataRdd.map(p => p.copy(features = pca.transform(p.features)))
    //val test_pca = test.map(p => p.copy(features = pca.transform(p.features)))
    val projected = trainDataRdd.map(p => p.copy(features = pca.transform(p.features)))
    println("output5：")*/

    //训练模型
   val model = NaiveBayes.train(trainDataRdd.rdd, lambda = 1.0, modelType = "multinomial")


    //测试数据集，做同样的特征表示及格式转换
    val testwordsData = tokenizer.transform(testDF)
    val testfeaturizedData = hashingTF.transform(testwordsData)
    val testrescaledData = idfModel.transform(testfeaturizedData)
    val testDataRdd = testrescaledData.select($"category",$"features").map {
      case Row(label: String, features: org.apache.spark.ml.linalg.Vector) =>
        org.apache.spark.mllib.regression.LabeledPoint(label.toDouble, org.apache.spark.mllib.linalg.Vectors.dense(features.toArray))
    }

    //对测试数据集使用训练模型进行分类预测
    val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))
    //testpredictionAndLabel.foreach(println)
    //统计分类准确率
    val testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()
    println("output5：")
    println(testaccuracy)

     model.save(sc,"/Users/b/Documents/andlinks/model")


  }
  }
