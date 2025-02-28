# import the necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Create a SparkSession
spark = (SparkSession
         .builder
         .appName("AuthorsAges")
         .getOrCreate())

# Create a DataFrame
data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30),
                                 ("TD", 35), ("Brooke", 25)], ["name", "age"])

# group the same names together, aggregate their ages and compute an average
avg_df = data_df.groupby("name").agg(avg("age"))

# Show the results
avg_df.show()

