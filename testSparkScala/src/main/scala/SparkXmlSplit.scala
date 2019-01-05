import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.databricks.spark.xml._

//import org.apache.spark._
import org.apache.spark.sql.types._


object testXMLSpark2 {
  def main(args: Array[String]) {
    val spark = SparkSession.builder
                            .appName("Simple Application")
                            .config("spark.master", "local[*]")
                            .getOrCreate()

    val xmlFile = "D:/GitProjects/testPythonProject/PyTest/data/test1.xml"

    val df = spark.read
      .option("rootTag", "LOG")
      .option("rowTag", "rec")
      .xml(xmlFile)
    //df.printSchema()
    //df.show()
    
    val distDF = df.select("_id").distinct()
    distDF.show()
    
    //distDF.foreach(t => println("D:/SPARK/data/test1_" + t.getString(0) + ".xml"))
    
    distDF.collect().map(t => {df.filter( df("_id") === t.getString(0) )
                                 .write
                                 .option("rootTag", "LOG")
                                 .option("rowTag", "rec")
                                 .mode("overwrite")
                                 .xml("D:/SPARK/data/test1_" +  t.getString(0) + ".xml")
                              }
                        )

    /*
    val selectedData = df.filter(df("_id") === "AA")
    selectedData.show()
    
    selectedData.write
      .option("rootTag", "LOG")
      .option("rowTag", "rec")
      .mode("overwrite")
      .xml("D:/SPARK/data/test1_AA.xml")
      * 
      */

     spark.stop()
     
  }
  
}
