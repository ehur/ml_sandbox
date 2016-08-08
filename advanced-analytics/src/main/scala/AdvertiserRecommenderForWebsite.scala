import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkContext, SparkConf}

class AdvertiserRecommenderForWebsite {
  //which advertisers do we recommend to a website?
  /* ./bin/spark-shell ----master yarn --driver-memory 12G --executor-memory 2G --driver-java-options "-Djava.io.tmpdir=/home/cjhadoop"*/

  val conf = new SparkConf
  val sc = new SparkContext(conf)

  val rawData = sc.textFile("lizDocs/website_advertiser_rating.csv") //all websites that already have advertisers. need set of websites for whom there is no advertiser yet?


  val trainData = rawData.map {line =>
    val Array(websiteId, advertiserId, feeRevenue) = line.split(',')
    Rating(websiteId.toInt,advertiserId.toInt,feeRevenue.toDouble)
  }

  val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)

  val recommendations = model.recommendProducts(3066968, 10)
  recommendations.foreach(println)


}
