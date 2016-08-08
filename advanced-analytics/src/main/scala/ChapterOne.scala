import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.lang.Double.isNaN
import org.apache.spark.util.StatCounter

class ChapterOne extends App {

  val conf = new SparkConf
  val sc = new SparkContext(conf)
  val rawblocks=sc.textFile("file:///home/lhurley/git/ml_sandbox/linkage/blocks/block*.csv")

  val noheader=rawblocks.filter(!isHeader(_))
  val parsed=noheader.map(line=>parse(line))
  parsed.cache()

  val matchCounts = parsed.map(md => md.matched).countByValue()

  val sampleStats=parsed.map(md => md.scores(0)).filter(!isNaN(_)).stats()

  val nasRDD= parsed.map(md => md.scores.map(d => NAStatCounter(d)))

  val reduced=nasRDD.reduce((n1,n2)=>{n1.zip(n2).map {case(a,b)=>a.merge(b)}})

  def isHeader(line:String) = line.contains("id_1")

  def toDouble(s:String) = {
    if("?".equals(s)) Double.NaN else s.toDouble
  }

  def parse(line:String) = {
    val pieces=line.split(",")
    val id1 = pieces(0).toInt
    val id2 = pieces(1).toInt
    val scores=pieces.slice(2,11).map(toDouble)
    val matched=pieces(11).toBoolean
    MatchData(id1,id2,scores,matched)
  }

  def statsWithMissing(rdd:RDD[Array[Double]]): Array[NAStatCounter] = {
    val nastats: RDD[Array[NAStatCounter]] = rdd.mapPartitions((iter:Iterator[Array[Double]]) => {
      val nas:Array[NAStatCounter] = iter.next().map(d => NAStatCounter(d))       //gets the next Array[Double]
      iter.foreach(arr => { //loops over remaining Arrays
        nas.zip(arr).foreach {case(n,d) => n.add(d)}
      })
      Iterator(nas)
    })
    nastats.reduce((n1,n2) => {
      n1.zip(n2).map{ case(a,b) => a.merge(b) }
    })
  }

}

case class MatchData(id1:Int,id2:Int,scores:Array[Double],matched:Boolean)

class NAStatCounter extends Serializable {
  val stats: org.apache.spark.util.StatCounter = new StatCounter()
  var missing: Long = 0

  def add(x:Double): NAStatCounter = {
    if(isNaN(x)) {
      missing += 1
    } else {
      stats.merge(x)
    }
    this
  }

  def merge(other:NAStatCounter): NAStatCounter = {
    stats.merge(other.stats)
    missing += other.missing
    this
  }

  override def toString = {
    "stats: " + stats.toString() + " NaN: " + missing
  }

}

object NAStatCounter extends Serializable {
  def apply(x:Double) = new NAStatCounter().add(x)
}