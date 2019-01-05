import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql._
import org.apache.spark.sql.types._



object testJSON {

  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    /*
    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files
    val path = "C:/SPARK/spark-2.4.0-bin-hadoop2.7/examples/src/main/resources/people.json"
    val peopleDF = spark.read.json(path)

    // The inferred schema can be visualized using the printSchema() method
    peopleDF.printSchema()
    peopleDF.show()

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by spark
    val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    teenagerNamesDF.show()
    *
    */

    // Alternatively, a DataFrame can be created for a JSON dataset represented by
    // a Dataset[String] storing one JSON object per string
    val otherPeopleDataset = spark.createDataset(
      """{"name":"Yin" ,"address":{"city":"Columbus","state":"Ohio" }} """ ::
      """{"name":"John","address":{"city":"Hartford","state":"Texas"}} """ ::
         Nil)

    val otherPeople = spark.read.json(otherPeopleDataset)
    otherPeople.show()

    spark.close()
  }

}