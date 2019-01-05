import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.databricks.spark.xml._


object splitXML {

  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val inputDir  = "C:/EclipseNeonWorkSpace/OmniXML/data/"
    val inputFile = "test1.xml"

    val outputDir = "C:/EclipseNeonWorkSpace/OmniXML/data/SparkOutput/"
    val outputFilePrefix = "test1_"
    val outputFileSuffix = ".xml"

    val inputXMLDF = spark.read
                     .option("roottag", "LOG")
                     .option("rowTag" , "LgRec")
                     .xml(inputDir+inputFile)

    //inputXMLDF.printSchema()
    //inputXMLDF.show()

    val recTypeDF = inputXMLDF.select("_RecId").distinct()
    //recTypeDF.printSchema()
    //recTypeDF.show()

    recTypeDF.collect().map(row => {
                                     println("row = " + row(0) )
                                     inputXMLDF.filter(inputXMLDF("_RecId")===row(0))
                                               .write
                                               .option("roottag", "LOG")
                                               .option("rowTag" , "LgRec")
                                               .mode("overwrite")
                                               .xml(outputDir + outputFilePrefix + row(0) + outputFileSuffix)

                                   })

    spark.close()
  }

}