#import the necessary libraries
import sys
import findspark
from pyspark.sql import SparkSession

findspark.init()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount<file>", file=sys.stderr)
        sys.exit(-1)

# Build a spark session using the spark session APIs
# If one does not exist then create an instance. There can only be one
# Spark session per JVM

spark = (SparkSession
         .builder
         .appName("PythonMnMCount")
         .getOrCreate())

# Get the dataset
mnm_file = sys.argv[1]
# Read the file into a Spark DataFrame using the CSV
# format by inferring the schema and specfying that the
# file contains a header, which provides column names for comma-
# separated fileds.
mnm_df = (spark.read.format("CSV")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(mnm_file))

# 1. Select from the DataFrame the fields "State", "Color" and "Count"
# 2. Group each state and its M&M color count using "groupBy()"
# 3. Aggregate counts of all colors and goupBy() State and Color
# 4. orderBy() in descending order

count_mnm_df = (mnm_df
                .select("State", "Color", "Count")
                .groupBy("State", "Color")
                .sum("Count")
                .orderBy("sum(Count)", ascending=False))
# Show the resulting aggregations for all the states and colors
# a total count of each color per state.

count_mnm_df.show(n=60, truncate=False)
print("Total Rows = %d" % (count_mnm_df.count()))

# data for CA only
# 1. select from all rows in the DataFrame
# 2. Filter only CA state
# 3. groupBy() State and Color
# 4. Aggregate the counts for each color
# 5. orderBy() in descending order

ca_count_mnm_df = (mnm_df
                   .select("State", "Color", "Count")
                   .where(mnm_df.State == "CA")
                   .groupBy("State","Color")
                   .sum("Count")
                   .orderBy("sum(Count)",ascending=False))
# Show the resulting aggregation in CA
ca_count_mnm_df.show(n=10, truncate=False)

#Stop the Spark Session
spark.stop()

