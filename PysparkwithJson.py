from pyspark.sql import SparkSession


def main():

    spark = SparkSession.builder.getOrCreate()
    Data_path = r"D:\Python_Code\PySparkProject\PysparkCode\Data"
    File_path = Data_path + r"\utilization.json"
    df2 = spark.read.format("json").load(File_path)
    # df2_sample = df2.sample(False, fraction=0.1)

    # df2.show()
    spark.stop()




















if __name__ == "__main__":
    main()