#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # Initialize Spark Session
    spark = SparkSession.builder         .appName("Crime Event Analysis")         .getOrCreate()

    # Read the data
    df = spark.read.csv("/user/xw2207_nyu_edu/merge_dataset/part-00000-3704f7da-210d-4501-802a-c723293ea8f1-c000.csv", header=True, inferSchema=True, multiLine=True, quote='"', escape='"')

    # Show the DataFrame to verify it's loaded correctly
    df.show()

    # Count of Crimes by Event Type
    crime_count_by_event_type = df.groupBy("Event Type").count()

    # Renaming the 'count' column to 'CrimeCount' to make the output clearer
    crime_count_by_event_type = crime_count_by_event_type.withColumnRenamed("count", "Crime Count")

    # Show the results
    crime_count_by_event_type.show(truncate=False)

    # Filter data for only 'FELONY' level offenses
    felony_df = df.filter(col("Level Of Offense") == "FELONY")

    # Group by 'Event Type' and count felony occurrences
    felony_count_by_event_type = felony_df.groupBy("Event Type").count()

    # Rename the 'count' column to 'FelonyCount' for clarity
    felony_count_by_event_type = felony_count_by_event_type.withColumnRenamed("count", "Felony Count")

    # Display the results sorted by the number of felonies
    felony_count_by_event_type.orderBy(col("Felony Count").desc()).show(truncate=False)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()

