from pyspark.sql import SparkSession


def main():

    # Create a Spark session
    spark = SparkSession.builder.getOrCreate()

    # Read a CSV file into a DataFrame
    Data_path = r"D:\Python_Code\PySparkProject\PysparkCode\Data"
    File_path = Data_path + r"\location_temp.csv"
    df1 = spark.read.format("csv").option("header", "true").load(File_path)
    # df2 = spark.read.format("csv").option("header", "false").option("inferSchema", "true").load(File_path)
    # df2 = df2.withColumnRenamed("_c0", "event_datetime") \
    #     .withColumnRenamed("_c1", "server_id") \
    #     .withColumnRenamed("_c2", "cpu_utilization") \
    #     .withColumnRenamed("_c3", "free_memory") \
    #     .withColumnRenamed("_c4", "session_count")
    # print(df1.filter(df1["location_id"] == "loc1").count())
    # df1.orderBy("location_id").groupBy("location_id").count().show()
    # df1.groupby("location_id").agg({'temp_celcius':'mean'}).show()
    df_s1 = df1.sample(fraction=0.1, withReplacement=False)
    df_s2 = df1.sample(fraction=0.1, withReplacement=False)
    print(df_s1.count())
    print(df_s2.count())



    spark.stop()








if __name__ == "__main__":
    main()

