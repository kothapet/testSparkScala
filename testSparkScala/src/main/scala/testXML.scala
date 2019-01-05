import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.databricks.spark.xml._


object testXML {

  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val inputDir  = "C:/EclipseNeonWorkSpace/OmniXML/data/"
    val inputFile = "q1.xml"
    val outputDir = "C:/EclipseNeonWorkSpace/OmniXML/data/SparkOutput/"
    val outputFile = "o1.xml"

    val xmlDF = spark.read
                     .option("roottag", "RootTag")
                     .option("rowTag" , "RowTag")
                     .xml(inputDir+inputFile)

    xmlDF.printSchema()
    xmlDF.show()

    xmlDF.write
         .option("roottag", "RootTag")
         .option("rowTag" , "RowTag")
         .mode("overwrite")
         .xml(outputDir + outputFile)

    spark.close()
  }

}