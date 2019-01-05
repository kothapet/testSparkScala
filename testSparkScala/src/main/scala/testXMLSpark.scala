
/* SimpleApp.scala */
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.databricks.spark.xml._

//import org.apache.spark._
import org.apache.spark.sql.types._


object testXMLSpark {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
                            .appName("Simple Application")
                            .config("spark.master", "local[*]")
                            .getOrCreate()

    val xmlFile = "D:/SPARK/data/books.xml"
    val customSchema = StructType(Array(
            StructField("_id", StringType, nullable = true),
            StructField("author", StringType, nullable = true),
            StructField("description", StringType, nullable = true),
            StructField("genre", StringType, nullable = true),
            StructField("price", DoubleType, nullable = true),
            StructField("publish_date", DateType, nullable = true),
            StructField("title", StringType, nullable = true)))

    val df = spark.read
      .option("rowTag", "book")
      .schema(customSchema)
      .xml(xmlFile)
    df.printSchema()
    df.show()

    val selectedData = df.select("author", "_id", "publish_date")
    selectedData.write
      .option("rootTag", "books")
      .option("rowTag", "book")
      .mode("overwrite")
      .xml("D:/SPARK/data/newbooks.xml")
  
    spark.stop()
  }
}
