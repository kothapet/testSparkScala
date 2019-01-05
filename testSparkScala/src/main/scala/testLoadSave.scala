import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql._
import org.apache.spark.sql.types._


object testLoadSave {
 def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    /*
    val usersDF = spark.read.load("C:/SPARK/spark-2.4.0-bin-hadoop2.7/examples/src/main/resources/users.parquet")
    usersDF.printSchema()
    usersDF.show()

    usersDF.write.format("orc")
      .option("orc.bloom.filter.columns", "favorite_color")
      .option("orc.dictionary.key.threshold", "1.0")
      .mode("overwrite")
      .save("output/users_with_options.orc")

    val usersDF2 = spark.read.format("orc").load("output/users_with_options.orc")
    usersDF2.printSchema()
    usersDF2.show()
      *
      */


    //usersDF.select("name", "favorite_color").write.save("output/namesAndFavColors.parquet")
    /*
    usersDF.select("name", "favorite_color").write.format("parquet").mode("overwrite").save("output/namesAndFavColors.parquet")

    val peopleDF = spark.read.format("json").load("C:/SPARK/spark-2.4.0-bin-hadoop2.7/examples/src/main/resources/people.json")
    peopleDF.select("name", "age").write.format("parquet").mode("overwrite").save("output/namesAndAges.parquet")

    val peopleDFCsv = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("C:/SPARK/spark-2.4.0-bin-hadoop2.7/examples/src/main/resources/people.csv")
    peopleDFCsv.printSchema()
    peopleDFCsv.show()
  	*/

    val sqlDF = spark.sql("SELECT * FROM parquet.`C:/SPARK/spark-2.4.0-bin-hadoop2.7/examples/src/main/resources/users.parquet`")
    sqlDF.printSchema()
    sqlDF.show()

    spark.close()
  }

}