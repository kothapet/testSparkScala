import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataTypes, StructType}

package object AddColumns {

  def addColumnsFold(df: DataFrame, columns: List[String]): DataFrame = {
    import df.sparkSession.implicits._
    columns.foldLeft(df)((acc, col) => {
      acc.withColumn(col, acc("incipit").as[String].contains(col))
    })
  }

  private val mappingRows: StructType => List[String] => Row => Row = (schema) => (words) => (row) => {
    //val addedCols: List[Boolean] = words.map(word => row.getString(schema.fieldIndex("incipit")).contains(word))
    //val idx = schema.count(x => true)-1
    val addedCols: List[String] = words.map(word => null)
    Row.merge(row, Row.fromSeq(addedCols))
  }

  private def getSchema(df: DataFrame, words: List[String]): StructType = {
    var schema: StructType = df.schema
    //words.foreach(word => schema = schema.add(word, DataTypes.BooleanType, false))
    words.foreach(word => schema = schema.add(word, DataTypes.StringType, true))
    schema
  }

  def addColumnsViaMap(df: DataFrame, words: List[String]): DataFrame = {
    df.map(mappingRows(df.schema)(words))(RowEncoder.apply(getSchema(df, words)))
  }

  def evaluate(df: DataFrame) = df.take(1)

}