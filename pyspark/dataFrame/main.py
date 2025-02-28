import mySparkSession
import myData
from pyspark.sql.functions import *

def main():
    spark = mySparkSession.createSession('dataFrame')
    blogs_df = myData.createData(spark)
    blogs_df.orderBy(col("Id").desc()).show()
    blogs_df.select((expr("Hits * 2"))).show()
    ((blogs_df.withColumn("AuthorsID", (concat(expr("First"), expr("Last"), expr("Id"))))
     .select(col("AuthorsID")))
     .show())
    spark.stop()

if __name__ == '__main__':
    main()