/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object testRDDs {
  def main(args: Array[String]) {
    val master = "local"
    val appName = "testRDDs"

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)

    val data = Array(1, 2, 3, 4, 5, 6 )
    val distData = sc.parallelize(data, 10)

    val distFile = sc.textFile("D:/SPARK/spark-2.4.0-bin-hadoop2.7/README.md", 8)
    println( "length = " + distFile.map(rec => rec.length).reduce((a, b) => a + b))

    val lineLengths = distFile.map(s => s.length)
    lineLengths.persist()
    val totalLength = lineLengths.reduce((a, b) => a + b)
    println( "length = " + totalLength)


    lineLengths.collect().foreach(println)
    lineLengths.take(10).foreach(println)

    val words = distFile.flatMap(rec => rec.split("[ ,\t\n]+") ).filter(x => (x.trim() != "") )
    val pairs = words.map(str => (str, 1))
    pairs.persist()
    val strCount = pairs.reduceByKey((a,b) => a+b )
    val sortCount = strCount.sortBy(pair => pair._2, false)
    sortCount.collect().foreach(println)


    val leftSide  = sc.parallelize(Seq (   (1, "text1"), (2, "text2"), (3, "text3")     ))
    val rightSide = sc.parallelize(Seq (   (1, "fool1"), (2, "fool2"), (4, "fool4")     ))
    val fullJoin = leftSide.fullOuterJoin(rightSide).sortByKey()
    fullJoin.collect().foreach(println)

    var counter = 0
    lineLengths.foreach(x => counter += x)
    println("counter = " + counter )

    val accum = sc.longAccumulator("My Accumulator")
    lineLengths.foreach(x => accum.add(x))
    println("accum = " + accum )


    sc.stop()
  }
}
