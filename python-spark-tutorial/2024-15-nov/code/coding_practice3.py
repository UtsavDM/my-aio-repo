from pyspark.sql import SparkSession

spark=SparkSession.builder \
        .appName("practice2") \
        .master("local[2]") \
        .getOrCreate()

path="C:/Users/utsav/OneDrive/Documents/Personal/my_repo/my-aio-repo/python-spark-tutorial/2024-15-nov/dataset"
file="dataset_for_practice3.txt"

sc=spark.sparkContext

rdd1=sc.textFile(f"{path}/{file}")

n=0
for i in rdd1.take(1):
    for j in i:
        if j=='|':
            n+=1
        if n%5==0:        
            print(i)
    print(".")

# spark.read.format().options(delimiter="|")
