import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, initcap, lower, when}

object CrimeDataCleanup {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Crime Data Cleanup")
      .getOrCreate()
    println("Spark session started.")

    // Load the DataFrame from CSV file
    val filePath = "NYC_crime.csv"
    val df = spark.read.option("header", "true").csv(filePath)
    println("DataFrame loaded from CSV.")

    // Function to drop columns by index
    def dropColumnsByIndex(df: DataFrame, indices: Seq[Int]): DataFrame = {
      val columnNames = df.columns
      val columnsToDrop = indices.map(index => columnNames(index))
      df.drop(columnsToDrop: _*)
    }

    // Drop the specified columns
    val indicesToDrop = Seq(0, 1, 5, 6, 7, 8, 9, 10, 11, 12, 15, 16, 17, 18, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 35, 36, 37, 38, 39, 40)
    val droppedDf = dropColumnsByIndex(df, indicesToDrop)
    println("Columns dropped.")

    // Rename the columns
    val renamedDf = droppedDf
      .withColumnRenamed("BORO_NM", "Borough")
      .withColumnRenamed("CMPLNT_FR_DT", "Date")
      .withColumnRenamed("CMPLNT_FR_TM", "Time")
      .withColumnRenamed("LAW_CAT_CD", "Level of Offense")
      .withColumnRenamed("LOC_OF_OCCUR_DESC", "Location Description")
      .withColumnRenamed("PD_DESC", "Crime Description")
      .withColumnRenamed("X_COORD_CD", "X coordinate")
      .withColumnRenamed("Y_COORD_CD", "Y coordinate")
      .withColumnRenamed("Latitude", "Latitude")
      .withColumnRenamed("Longitude", "Longitude")
      .withColumnRenamed("Lat_Lon", "Lat_Lon")
    println("Columns renamed.")

    // Adjust text formatting in 'Borough'
    val adjustedDf = renamedDf
      .withColumn("Borough", initcap(lower(col("Borough"))))
      .withColumn("Borough", when(col("Borough") === "Staten island", "Staten Island").otherwise(col("Borough")))
    println("Text formatting adjusted for 'Borough'.")

    // Ensure that only existing columns are included in the reordering
    val existingColumns = Seq("Borough", "Date", "Time","Level of Offense", "Location Description", "Crime Description", "Latitude", "Longitude", "Lat_Lon")

    // Reorder the columns
    val reorderedDf = adjustedDf.select(existingColumns.map(col): _*)
    println("Columns reordered.")

    // Write the final DataFrame to a CSV file
    val outputFilePath = "NYC_crime_cleaned"
    reorderedDf.write.option("header", "true").csv(outputFilePath)
    println("DataFrame written to CSV.")

    // Stop the Spark session
    

    println("Spark session stopped.")
  }
}
