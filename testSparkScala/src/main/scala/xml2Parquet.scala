import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.databricks.spark.xml._
import org.apache.spark.sql.functions._

object xml2Parquet {

  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val inputDir  = "C:/EclipseNeonWorkSpace/OmniXML/data/SparkOutput/"
    val inputFile = "test1_AA.xml"

    val inputXMLDF = spark.read
                     .option("roottag", "LOG")
                     .option("rowTag" , "LgRec")
                     .xml(inputDir+inputFile)


    inputXMLDF.printSchema()
    //inputXMLDF.show()

    val df2 = inputXMLDF.withColumn("rowId", monotonically_increasing_id)
    df2.printSchema()
    //df2.show()

    val df3 = df2.select($"_RecId", $"rowId", explode($"UpdRec.field").as("fldals") )
                 .select(col("_RecId").as("RecId"),
                         col("rowId").as("RowId"),
                         col("fldals._num").as("fldId"),
                         col("fldals._Value").as("fldVal") )
    df3.printSchema()
    //df3.show()

    val df4 = df3.groupBy("RecId", "RowId")
                 .pivot("fldId")
                 .agg(max("fldVal"))
    df4.printSchema()
    df4.show()
    /*
    df4.withColumnRenamed("1", "Field 1")
       .withColumnRenamed("8", "Field 8")
       .show()
       *
       */

    val schemaDir  = "C:/EclipseNeonWorkSpace/OmniXML/data/meta/"
    val schemaFile = "AA.xml"

    val schemaDF = spark.read
                        .option("rowTag" , "DE")
                        .xml(schemaDir+schemaFile)

    schemaDF.printSchema()
    schemaDF.show()
    /*
    schemaDF.collect().map(row => { println("_num = " + row(4) +
                                            "; _Name = " + row(0) +
                                            "; _fieldtype = " + row(2) +
                                            "; _Opic = " + row(1)
                                           )
                                  })
                                  *
                                  */


    //val list = Map("Name" -> "name", "Position" -> "position", "Office" -> "office", "Age" -> "age", "StartDate" -> "startDate")
    //val result1 = df.select(list.map(x => col(x._1).alias(x._2)).toList : _*)

    /*
    val list = schemaDF.collect().map(row => { println("Val : " + row(4).toString())
                                               println("Bool: " + df4.columns.contains(row(4).toString()))
                                               if (!df4.columns.contains(row(4).toString()))
                                                  row(4).toString()
                                              })
                                              *
                                              */

    println(" conatins 1 : " + df4.columns.contains("1") )
    println(" conatins 8 : " + df4.columns.contains("8") )

    //schemaDF.filter( df4.columns.contains(col("_num"))  ).show()
    val list1 = schemaDF.collect().map(row => { if (!df4.columns.contains(row(4).toString()))
                                                  row(4).toString()
                                                else
                                                  null
                                              }).toList.filter(x => x != null)
    println(list1)

    //println("fieldIndex = " + df4.schema.fieldIndex("7"))
    //println("fieldount = " + df4.schema.count(x => true))

    val df5 = AddColumns.addColumnsViaMap(df4, list1)
    df5.printSchema()
    //df5.show()

    val schemaMap1  = Array(("RecId","RecId"), ("RowId","RowId"))
    val schemaMap2  = schemaDF.collect().map(row => (row(4).toString(), row(0).toString()))
    val schemaMap = schemaMap1 ++ schemaMap2
    schemaMap.foreach(println)

    val df6 = df5.select(schemaMap.map(row => col(row._1 ).alias(row._2 )).toList : _*)
    df6.printSchema()
    df6.show()
    /*
    val df6 = df5.select(schemaDF.collect().map(row => col(row(4).toString() ).alias(row(0).toString() )).toList : _*)
    df6.printSchema()
    df6.show()
    *
    */

    spark.close()
  }

}