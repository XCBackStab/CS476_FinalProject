import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, trim}

object DataFrameOperations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("DataFrame Operations")
      .getOrCreate()

    // Read CSV files into DataFrames
    val data1 = spark.read.option("header", "true").csv("final_cleaned")
    data1.show()
    val data2 = spark.read.option("header", "true").csv("NYC_crime_cleaned.csv")
    data2.show()
    // Strip spaces from all string columns in data1
    val data1_stripped = data1.columns.foldLeft(data1) { (df, columnName) => df.withColumn(columnName, trim(col(columnName)))}

    // Strip spaces from all string columns icn data2
    val data2_stripped = data2.columns.foldLeft(data2) { (df, columnName) =>df.withColumn(columnName, trim(col(columnName)))}

    // Perform an inner join on 'Borough' and 'Date'
    val result = data1_stripped.select("Borough", "Date", "Event Type", "Event Location").join(data2_stripped.select("Borough", "Date", "Level of Offense", "Crime Description"),Seq("Borough", "Date"),"inner")

    // Drop duplicates based on 'Borough', 'Date', 'Level of Offense', and 'Crime Description'
    val result_unique = result.dropDuplicates(Seq("Borough", "Date", "Level of Offense", "Crime Description"))

    // Show unique results
    result_unique.show()
    result_unique.coalesce(1).write.option("header", "true").csv("/user/xw2207_nyu_edu/merge_dataset")


  }
}
