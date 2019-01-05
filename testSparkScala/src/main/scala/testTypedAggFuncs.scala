import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._
import org.apache.spark.sql.types._

case class Employee(name: String, salary: Long)
case class Average(var sum: Long, var count: Long)

object MyAverage2 extends Aggregator[Employee, Average, Double] {
  // A zero value for this aggregation. Should satisfy the property that any b + zero = b
  def zero: Average = Average(0L, 0L)
  // Combine two values to produce a new value. For performance, the function may modify `buffer`
  // and return it instead of constructing a new object
  def reduce(buffer: Average, employee: Employee): Average = {
    buffer.sum += employee.salary
    buffer.count += 1
    buffer
  }
  // Merge two intermediate values
  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }
  // Transform the output of the reduction
  def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count
  // Specifies the Encoder for the intermediate value type
  def bufferEncoder: Encoder[Average] = Encoders.product
  // Specifies the Encoder for the final output value type
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}


object testTypedAggFuncs {
  def main(args: Array[String]) = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

		// For implicit conversions like converting RDDs to DataFrames
		import spark.implicits._

    val ds = spark.read.json("C:/SPARK/spark-2.4.0-bin-hadoop2.7/examples/src/main/resources/employees.json").as[Employee]
    ds.show()

    // Convert the function to a `TypedColumn` and give it a name
    val averageSalary = MyAverage2.toColumn.name("average_salary")
    val result = ds.select(averageSalary)
    result.show()

    spark.close()
  }
}