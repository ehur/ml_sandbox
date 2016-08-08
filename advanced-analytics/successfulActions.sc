import org.apache.spark.{SparkContext, SparkConf}
import com.cj.protocol.tracking.SuccessfulAction


var conf = new SparkConf
conf.setMaster("local")
conf.setAppName("tester")
val sc = new SparkContext(conf)

val actions = sc.textFile("/user/cjhadoop/datawarehouse/raw/ap/successfulActions/byLogDate/20150801")
