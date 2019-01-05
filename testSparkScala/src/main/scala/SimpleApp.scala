/* SimpleApp.scala */
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
//import org.apache.spark._
//import org.apache.spark.sql.types._


object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "D:/SPARK/spark-2.4.0-bin-hadoop2.7/README.md" // Should be some file on your system
    val spark = SparkSession.builder
                            .appName("Simple Application")
                            .config("spark.master", "local[*]")
                            .getOrCreate()


    //val sc = spark.sparkContext
    val logData = spark.read.textFile(logFile).cache()
    val lc = logData.count()
    println(s"********* Line count: $lc ************")
    val first = logData.first()
    println(s"********* first: $first ************")
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"********* Lines with a: $numAs, Lines with b: $numBs ************")

    import spark.implicits._

    //val maxWordSize = logData.map(line => line.split(" ").size )
    val maxWordSize = logData.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)
    println(s"********* max word size: $maxWordSize ************")
    val wordCounts = logData.map(line => line.split(" ").size).reduce((a, b) => Math.max(a, b))
    println(s"********* max word size: $wordCounts ************")
    val wordCounts2 = logData.flatMap(line => line.split(" ")).groupByKey(identity).count()
    //wordCounts2.collect()
    wordCounts2.printSchema
    wordCounts2.sort(desc("count(1)")).show(30)
    //println(s"********* max word size: $wordCounts ************")    * */
    //spark.close() same as spark.stop() or sc.stop()
    spark.stop()
  }
}
