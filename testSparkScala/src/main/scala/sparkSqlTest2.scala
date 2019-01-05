import org.apache.spark.sql.SparkSession

case class Person(name: String, age: Long)

object sparkSqlTest2 {
	def main(args: Array[String]) = {

    val spark = SparkSession
					.builder()
					.appName("Spark SQL basic example")
					.master("local")
					.getOrCreate()

		// For implicit conversions like converting RDDs to DataFrames
		import spark.implicits._

		val caseClassDS = Seq(Person("Andy", 32)).toDS()
		caseClassDS.show()

		val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).show() // collect() // Returns: Array(2, 3, 4)

    val path = "C:/SPARK/spark-2.4.0-bin-hadoop2.7/examples/src/main/resources/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()

    spark.close()
	}
}