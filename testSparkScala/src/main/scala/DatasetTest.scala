import org.apache.spark.sql._

case class SimpleTuple(id: Int, desc: String)

object DatasetTest {

  //val dataList = List(1,2,3,4)

  val dataList = List(
    SimpleTuple(5, "abc"),
    SimpleTuple(6, "bcd")
  )

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .appName("DatasetTest")
      .getOrCreate()

    import sparkSession.implicits._
    val dataset = sparkSession.createDataset(dataList)
    dataset.printSchema()
    dataset.show(20)

  }

}
