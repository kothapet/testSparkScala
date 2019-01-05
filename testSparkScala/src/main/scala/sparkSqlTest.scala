import org.apache.spark.sql.SparkSession

object sparkSqlTest {
	def main(args: Array[String]) = {

    val spark = SparkSession
					.builder()
					.appName("Spark SQL basic example")
					.master("local")
					.getOrCreate()

		// For implicit conversions like converting RDDs to DataFrames
		import spark.implicits._
    val df = spark.read.json("C:/SPARK/spark-2.4.0-bin-hadoop2.7/examples/src/main/resources/people.json")

    df.cache()
    // Displays the content of the DataFrame to stdout
    df.printSchema()
    df.show()
    df.select("name").show()
    df.select($"name", $"age" + 1).show()

    df.filter($"age" > 21).show()

    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.printSchema()
    sqlDF.show()

    df.createGlobalTempView("people")
    spark.sql("SELECT * FROM global_temp.people").show()
    spark.newSession().sql("SELECT * FROM global_temp.people").show()

    spark.close()
	}
}