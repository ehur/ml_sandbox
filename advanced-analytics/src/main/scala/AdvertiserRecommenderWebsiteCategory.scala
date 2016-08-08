import breeze.numerics.log10
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

class AdvertiserRecommenderWebsiteCategory {
  //what advertisers would you recommend for my website category?
  /* ./bin/spark-shell --master yarn --driver-memory 12G --executor-memory 2G --driver-java-options "-Djava.io.tmpdir=/home/cjhadoop"
  * */

  val conf = new SparkConf
  val sc = new SparkContext(conf)

  val pubWebCatLookup = sc.textFile("lizDocs/pub_cat_lookup.csv")   //contains new publishers' websites and their categories
  val samplePubWebs = pubWebCatLookup.filter{line => line != "\"COMPANY\",\"ID\",\"CATEGORY\""}.take(4)  //eliminate header and take first 4

  val rawData = sc.textFile("lizDocs/web_cat_advertiser_rating.csv") //all websites that already have advertisers.
  val trimmedData = rawData.filter{line => line != "\"PARENT_CATEGORY_CATEGORY_CODE\",\"CHILD_CATEGORY_CATEGORY_CODE\",\"ADVERTISER_CID\",\"SUM(SUM_COMM_AMOUNT_USD)\""}.map{line =>
                                  val Array(parentCat,childCat,advCid,rating) = line.split(",")
                                  (childCat,advCid,rating)}    //eliminate header and  drop parentCat

  val trainData = trimmedData.map {case(childCat, advertiserId, rating) =>
    Rating(childCat.toInt,advertiserId.toInt,rating.toDouble)
  }
  val trainLogData = trimmedData.map {case(childCat, advertiserId, rating) =>
    Rating(childCat.toInt,advertiserId.toInt,log10(rating.toDouble))
  }

  //================================ evaluate model =======================
  val Array(train2Data,cvData) = trainLogData.randomSplit(Array(0.9,0.1))
  train2Data.cache()
  cvData.cache()

  val allAdvertiserIds = trainData.map(_.product).distinct().collect()
  val bAllAdvertiserIds = sc.broadcast(allAdvertiserIds)

  /*rank = 10 : The number of latent factors in the model, or equivalently, the number of columns k in the user-feature and product-feature matrices. In nontrivial cases, this is also their rank.
iterations = 5 : The number of iterations that the factorization runs. More iterations take more time but may produce a better factorization.
lambda = 0.01 : (regularization factor) A standard overfitting parameter. Higher values resist overfitting, but values that are too high hurt the factorization’s accuracy.
alpha = 1.0 : (learning rate) Controls the relative weight of observed versus unobserved user-product interactions in the factorization.
   */
  val trainModel = ALS.trainImplicit(train2Data, 10, 10, 0.5, 0.5)
  val auc = CodeUtils.areaUnderCurve(cvData,bAllAdvertiserIds,trainModel.predict)   //0.6697026500024117 first time thru
  // 0.8036237493576676 using log10(rating.toDouble) for val trainModel = ALS.trainImplicit(train2Data, 10, 5, 0.01, 1.0)
  // 0.8161506606207775 as above, but with 10 iterations
  //0.8310994202955765 as above but with 10 iterations, and lambda 0.5
  //0.8506952415241005 as above but 10 iterations, lambda 0.5, alpha 0.5
  //0.8282485007486714 as above with 20 iterations, lambda 0.5, alpha 0.5 -- boo! didn't help!
  //================================ evaluate model =======================

  val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)

  val newWebsites = samplePubWebs.map { line =>
    val Array(pubId,websiteId,categoryId)  = line.split(",")
    websiteId.toInt -> categoryId.toInt
  }

  var recommendationsMap = Map[Int,Array[Int]]()

  newWebsites.foreach {case(webId,catId) =>
    val recommendations = model.recommendProducts(catId, 10)
    recommendationsMap += webId -> recommendations
  }

  recommendationsMap.foreach(println)
}
