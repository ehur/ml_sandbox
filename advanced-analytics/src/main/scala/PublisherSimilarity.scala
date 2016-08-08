import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

class PublisherSimilarity {
  val conf = new SparkConf
  val sc = new SparkContext(conf)
  val websiteDescriptionsByPublisher = PublisherSimilarity.publisherWebsiteDescriptions(sc).cache()
  val programDescriptionsByPublisher = PublisherSimilarity.publisherProgramDescriptions(sc).cache()

  val words = """\b\p{L}+\b""".r    //regex to extract words. \p{L} is any kind of letter from any language. Find one or more occurrances of a letter/letters deliminated by space.


  lazy val globalTerms = {            //this is a global ordered set of all the words in the "corpus",
                                      //which is the words our pubs use to describe their websites and programs.
    var completeSet = Set[String]()
    for (x<- websiteDescriptionsByPublisher) {
      words findAllIn x._2 foreach {a => completeSet = completeSet + a}
    }
    for (x<- programDescriptionsByPublisher) {
      words findAllIn x._2 foreach {a => completeSet = completeSet + a}
    }
    completeSet.filter {_.length > 1}.toSeq           //remove words 1 character long
  }

  lazy val allDocuments = {
    websiteDescriptionsByPublisher.map(_._2) union programDescriptionsByPublisher.map(_._2)
  }

  def tfidfVectorByPublisher = {
    val publishers = websiteDescriptionsByPublisher.map(_._1) union programDescriptionsByPublisher.map(_._1)

  }

}

object PublisherSimilarity {

  val PUB_WEBSITE_DESCRIPTION_SQL="select distinct website.company, website.description\nfrom website,affiliate,company\nwhere website.company = affiliate.company\nand website.company = company.id\nand company.status = 0\nand network_classification is not null\nand company.preferred_language = 'en'"
  val PUB_PROGRAM_DESCRIPTION_SQL="select distinct sp.publisher_id, sp.description\nfrom special_program sp,affiliate,company\nwhere sp.publisher_id = affiliate.company\nand sp.publisher_id = company.id\nand company.status = 0\nand network_classification is not null\nand company.preferred_language = 'en'"

  def publisherWebsiteDescriptions(sc:SparkContext): RDD[(Long, String)] = { //one entry per publisher - all website descriptions are concatenated
    val rawData = sc.textFile("lizDocs/publisher_website_descriptions.csv")
    rawData.reduceByKey(_ + " " +_).collect()

//      rawData.map { line =>
//        val Array(publisherId,websiteDescription) = line.split(",",2)
//        publisherId.toLong -> websiteDescription
//      }
  }

  def publisherProgramDescriptions(sc:SparkContext) = {  //one entry per publisher - all prpogram descriptions are concatenated
    val rawData = sc.textFile("lizDocs/publisher_program_descriptions.csv")
    rawData.reduceByKey(_ + " " + _).collect()
//    rawData.map { line =>
//      val Array(publisherId,programDescription) = line.split(",",2)
//      publisherId.toLong -> programDescription
//    }
  }

}
