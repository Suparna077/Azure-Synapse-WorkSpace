from pyspark.sql import SparkSession




def main():
    spark = SparkSession \
        .builder \
        .appName("Spark SQL Query Dataframes") \
        .getOrCreate()

    Data_path = r"D:\Python_Code\PySparkProject\PysparkCode\Data"
    File_path = Data_path + r"\utilization.json"
    df_util = spark.read.format("json").load(File_path)
    df_util.createOrReplaceTempView("utilization")
    # df_util.describe().show()
    # df_util.describe("cpu_utilization","free_memory").show()
    # print(df_util.stat.corr('free_memory','cpu_utilization' ))
    df_util.stat.freqItems(('free_memory', 'cpu_utilization')).show(1)









if __name__ == "__main__":
    main()