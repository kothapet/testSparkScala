import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.databricks.spark.xml._
import org.apache.spark.sql.functions._

object testXML2 {

  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val inputDir  = "D:/SPARK/Data/"
    val inputFile = "testx.xml"

    val df1 = spark.read
                     .option("roottag", "Aggregator")
                     .option("rowTag" , "Session")
                     .xml(inputDir+inputFile)

    df1.printSchema()
    df1.show()
    
    val df2 = df1.select( explode($"Data.Log.Event").as("Event") )
    df2.printSchema()
    df2.show()
     
    val df3 = df2.filter(df2("Event.Start")==="")
    df3.printSchema()
    df3.show()
     
    spark.close()
  }

}