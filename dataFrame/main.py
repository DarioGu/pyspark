import mySparkSession
import myData

def main():
    spark = mySparkSession.createSession('dataFrame')
    myData.createData(spark)
    spark.stop()

if __name__ == '__main__':
    main()