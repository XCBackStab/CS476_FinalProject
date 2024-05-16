#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, first, concat, lit, to_timestamp, date_format, countDistinct

def main():
    # Initialize Spark Session
    spark = SparkSession.builder         .appName("DataCleaningExample")         .getOrCreate()

    # Read the CSV file into a DataFrame
    data = spark.read.csv("event_nyc.csv", header=True)

    # Calculate the number of missing values in each column
    for column in data.columns:
        missing_values = data.filter(col(column).isNull()).count()
        print(f"Missing values in {column}: {missing_values}")

    data.show(5)
    data.printSchema()

    # Drop the last 4 columns of the DataFrame
    data = data.drop(*data.columns[-4:])

    # Drop specific columns by index
    drop_columns = [data.columns[0]]
    drop_columns2 = [data.columns[2]]
    data = data.drop(*drop_columns)
    data = data.drop(*drop_columns2)

    data.show(5)

    data.groupBy("Event Agency").count().show()
    data.groupBy("Event Type").count().show()
    data.groupBy("Event Borough").count().show()

    # Counting distinct values in each specified categorical column
    data.select([countDistinct(c).alias(f"{c} distinct count") for c in ["Event Agency", "Event Type", "Event Borough"]]).show()

    # Renaming columns
    data = data.withColumnRenamed("Event Name", "Name")                .withColumnRenamed("Event Borough", "Borough")
    data.show(5)

    data = data.withColumn("End Date/Time", to_timestamp("End Date/Time", "MM/dd/yyyy hh:mm:ss a"))
    data = data.withColumn("End Date/Time", date_format("End Date/Time", "MM/dd/yyyy HH:mm:ss"))
    split_col = split(data["End Date/Time"], " ")
    data = data.withColumn("Date", split_col.getItem(0))
    
    data.show(5)

    # Filter DataFrame to only include events organized by the Parks Department
    park_events = data.filter(col("Event Agency") == "Parks Department")

    # Splitting and keeping the first part of the "Event Location" column based on ":"
    park_events = park_events.withColumn("Event Location", split(col("Event Location"), ":").getItem(0))
    park_events.show(5)

    # Aggregating the data
    aggregated_data = park_events.groupBy(
        "Event Agency", 'Date',
        "Event Type", "Borough", "Event Location"
    ).agg(
        first("Name").alias("Name")
    )

  
    # Select and show final data
    aggregated_data = aggregated_data.select(
        "Borough",
        "Date",
        "Name",
        "Event Type", 
        "Event Agency", 
        "Event Location"
    )
    
    aggregated_data.show(5)

    # Write the DataFrame as a single CSV file
    aggregated_data.coalesce(1).write.option("header", "true").csv("/user/xw2207_nyu_edu/final_cleaned")

    spark.stop()

if __name__ == "__main__":
    main()


# In[ ]:




