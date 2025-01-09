from pyspark.sql import SparkSession

spark=SparkSession.builder \
        .master("local[2]") \
        .appName("practice2") \
        .getOrCreate()

path="C:/Users/utsav/OneDrive/Documents/Personal/my_repo/my-aio-repo/python-spark-tutorial/2024-15-nov/dataset"
file="dataset_for_practice2.csv"

data_df=spark.read.format("csv").options(delimiter="~|",header=True,inferSchema=True).load(f"{path}/{file}")

data_df.printSchema()
data_df.show()
