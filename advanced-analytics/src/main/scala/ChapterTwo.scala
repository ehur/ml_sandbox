import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation._

class ChapterTwo {
  /* ./bin/spark-shell ----master yarn --driver-memory 12G --executor-memory 2G --driver-java-options "-Djava.io.tmpdir=/home/cjhadoop" <== WORKED! */

  val conf = new SparkConf
  val sc = new SparkContext(conf)

  //  val rawUserArtistData = sc.textFile("file:///home/lhurley/git/ml_sandbox/profiledata_06-May-2005/user_artist_data.txt")
  val rawUserArtistData = sc.textFile("lizDocs/ml/user_artist_data.txt")
  //  rawUserArtistData.map(_.split(' ')(0).toDouble).stats()
  //  val rawArtistData= sc.textFile("file:///home/lhurley/git/ml_sandbox/profiledata_06-May-2005/artist_data.txt")
  val rawArtistData = sc.textFile("lizDocs/ml/artist_data.txt")

  val artistByID = rawArtistData.flatMap { line =>
    val (id, name) = line.span(_ != '\t')
    if (name.isEmpty) {
      None
    } else {
      try {
        Some((id.toInt, name.trim))
      } catch {
        case e: NumberFormatException => None
      }
    }
  }

  //  val rawArtistAlias = sc.textFile("file:///home/lhurley/git/ml_sandbox/profiledata_06-May-2005/artist_alias.txt")
  val rawArtistAlias = sc.textFile("lizDocs/ml/artist_alias.txt")

  val artistAliasIntermediate = rawArtistAlias.flatMap { line =>
    val tokens = line.split('\t')
    if (tokens(0).isEmpty) {
      None
    } else {
      Some((tokens(0).toInt, tokens(1).toInt))
    }
  }.collectAsMap()

  val artistAlias = collection.immutable.Map(artistAliasIntermediate.toSeq:_*) //work around because broadcast requires immutable.Map

  val bArtistAlias: Broadcast[scala.collection.immutable.Map[Int,Int]] = sc.broadcast(artistAlias)           //works in spark console, if not here....

  val trainData = rawUserArtistData.map { line =>
    val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
    val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
    Rating(userID, finalArtistID, count)
  }.cache()

  /* * @param ratings    RDD of (userID, productID, rating) pairs
   * @param rank       number of features to use
   * @param iterations number of iterations of ALS (recommended: 10-20)
   * @param lambda     regularization factor (recommended: 0.01)
   * @param alpha     learning rate
   * */
  val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)

  val rawArtistsForUser = rawUserArtistData.map(_.split(' ')).filter {
    case Array(user, _, _) => user.toInt == 2093760
  }

  val existingProducts = rawArtistsForUser.map { case Array(_, artist, _) => artist.toInt }.collect().toSet

  artistByID.filter {
    case (id, name) => existingProducts.contains(id)
  }.values.collect().foreach(println)

  val recommendations = model.recommendProducts(2093760, 5)
  recommendations.foreach(println)
  val recommendedProductIDs = recommendations.map(_.product).toSet
  artistByID.filter { case (id,name) => recommendedProductIDs.contains(id)}.values.collect().foreach(println)

  val allData = CodeUtils.buildRatings(rawUserArtistData, bArtistAlias)

  val Array(trainData,cvData) = allData.randomSplit(Array(0.9,0.1))
  trainData.cache()
  cvData.cache()

  val allItemIDs = allData.map(_.product).distinct().collect()
  val bAllItemIDs = sc.broadcast(allItemIDs)

  val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)
  val auc = CodeUtils.areaUnderCurve(cvData, bAllItemIDs, model.predict)


}

