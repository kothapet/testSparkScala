
import org.apache.spark.sql._
import org.apache.spark.sql.types._


object testSpecifySchema {
	def main(args: Array[String]) = {

    val spark = SparkSession
					.builder()
					.appName("Spark SQL basic example")
					.master("local")
					.getOrCreate()

		// For implicit conversions like converting RDDs to DataFrames
		import spark.implicits._

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
		val rowRDD  = spark.sparkContext
      .textFile("C:/SPARK/spark-2.4.0-bin-hadoop2.7/examples/src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    // The schema is encoded in a string
    val schemaString = "name,age"
    val fields = schemaString.split(",")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // Apply the schema to the RDD
    val peopleDF = spark.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("SELECT *  FROM people")

    results.printSchema()
    results.show()

    def inferType(field: String) = field  match {
         case "Int" => IntegerType
         case "Double" => DoubleType
         case "String" => StringType
         case _ => StringType
      }

    // The schema is encoded in a string
    val schemaSeq = "name:String,age:Int"
    val schema1 = StructType(schemaSeq.split(",").map(column => StructField(column.split(":")(0), inferType(column.split(":")(1)), true)))

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
		val rowRDD1  = spark.sparkContext
      .textFile("C:/SPARK/spark-2.4.0-bin-hadoop2.7/examples/src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim.toInt))

    // Apply the schema to the RDD
    val peopleDF1 = spark.createDataFrame(rowRDD1, schema1)
    // Creates a temporary view using the DataFrame
    peopleDF1.createOrReplaceTempView("people1")

    // SQL can be run over a temporary view created using DataFrames
    val results1 = spark.sql("SELECT *  FROM people1")

    results1.printSchema()
    results1.show()

    spark.close()
	}
}