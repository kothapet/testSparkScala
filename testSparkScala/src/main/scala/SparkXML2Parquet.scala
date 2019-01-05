import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.databricks.spark.xml._
import org.apache.spark.sql.types._


//import org.apache.spark._
import org.apache.spark.sql.types._


object SparkXML2Parquet {

  def main(args: Array[String]) {

    val spark = SparkSession.builder
                            .appName("Simple Application")
                            .config("spark.master", "local[*]")
                            .getOrCreate()
    import spark.implicits._

    val metaXmlFile = "D:/GitProjects/testPythonProject/PyTest/data/meta_AA.xml"
    val metaDF = spark.read
      .option("rowTag", "DE")
      .xml(metaXmlFile)
      
    metaDF.printSchema()
    metaDF.show()
    //metaDF.select("_Name").where(metaDF("_num") === 1).show()
    //val colMap: Map[Long, String] = metaDF.collect().map(t  => t(0) )
    
    /*
    val xmlFile = "D:/SPARK/data/test1_AA.xml"

    val schemaDef = StructType(Array(
                               StructField("_id", StringType, nullable = true) ,
                               StructField("field", ArrayType(StructType(Array( StructField("_VALUE", StringType, nullable = true) ,
                                                                                StructField("_id", StringType, nullable = true) 
                                                                              )
                                                                         )
                                                             ), nullable = true)  
                              ) )
    
    val df = spark.read
      .option("rootTag", "LOG")
      .option("rowTag", "rec")
      .schema(schemaDef)
      .xml(xmlFile)
    df.printSchema()
    df.show()
    
    val flatDF = df.withColumn("rowId", monotonically_increasing_id()) .select( col("_id"), col("rowId"), explode(col("field")).as("col1") )
    flatDF.printSchema()
    flatDF.show()
    
    val splitDF = flatDF.select( col("_id"), col("rowId"), col("col1._id").as("col_idx"), col("col1._VALUE").as("col_val") )
    splitDF.printSchema()
    splitDF.show()
    
    val normDF = splitDF.groupBy("_id", "rowId").pivot("col_idx").agg(first("col_val"))
    normDF.printSchema()
    normDF.show()
    * 
    */
    

    spark.stop()
     
  }
  
}
