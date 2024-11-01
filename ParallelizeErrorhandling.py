from pyspark.shell import sc
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
import findspark
import logging



def main():
    try:

        spark = (SparkSession \
                 .builder \
                 .appName("ParallelizeTest") \
                 .master("Local[*]") \
                 .enableHiveSupport() \
                 .getOrCreate())
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    try:
        df = spark.sparkContext.parallelize([1,2,3,5,6])

        print(df.count())
    except Exception as e:
        print(f"An error occurred DF: {str(e)}")






if __name__ == "__main__":
    main()
