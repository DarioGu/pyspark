from pyspark.sql import SparkSession

def createSession(appname: str):
    spark_sess = (SparkSession
         .builder
         .appName(appname)
         .getOrCreate())
    print('Session created', spark_sess)
    return spark_sess
