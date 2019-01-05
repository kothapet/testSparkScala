import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql._
import org.apache.spark.sql.types._



object testParquet {

  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    /*
    val peopleDF = spark.read.json("C:/SPARK/spark-2.4.0-bin-hadoop2.7/examples/src/main/resources/people.json")

    // DataFrames can be saved as Parquet files, maintaining the schema information
    peopleDF.write.mode("overwrite").parquet("output/people.parquet")

    // Read in the parquet file created above
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a Parquet file is also a DataFrame
    val parquetFileDF = spark.read.parquet("output/people.parquet")

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.printSchema()
    namesDF.show()
    *
    */

    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    squaresDF.write.mode("overwrite").parquet("output/data/test_table/key=1")

    val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i*i, i * i * i)).toDF("value", "square", "cube")
    cubesDF.write.mode("overwrite").parquet("output/data/test_table/key=2")

    val mergedDF = spark.read.option("mergeSchema", "true").parquet("output/data/test_table")
    mergedDF.printSchema()
    mergedDF.show()

    spark.close()
  }

}