# pyspark word-count problem
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

path="C:\\Users\\utsav\\OneDrive\\Documents\\Personal\\my_repo\\my-aio-repo\\python-spark-tutorial\\2024-15-nov\\dataset"
file="deliveries.csv"
try:
    spark = SparkSession.builder \
                        .master("local[2]") \
                        .appName("word count problem") \
                        .getOrCreate()

    sc = spark.sparkContext
    print("Spark session initialized! ")
    read_file=sc.textFile(f"{path}\{file}")
    # read_file.take(1)
    header_rdd=read_file.first()
    data_rdd=read_file.filter(lambda x: x!=header_rdd)
    data_rdd=data_rdd.map(lambda x: re.sub(r'[\d]+'," ",x))
    parsed_rdd = data_rdd.flatMap(lambda x: x.split(",")).flatMap(lambda x: x.split(" ")).filter(lambda x:x not in (" ",""))
    
    word_mapped_rdd = parsed_rdd.map(lambda x:(x,1))
    print("words tagged with 1",word_mapped_rdd.take(2))
    
    count_rdd = word_mapped_rdd.reduceByKey(lambda x,y:x+y)
    print("count_rdd: ",count_rdd.take(2))
    
    reversed_rdd = count_rdd.map(lambda x:(x[1],x[0]))
    print("reversed_rdd: ",reversed_rdd.take(2))
    
    for row in reversed_rdd.sortByKey(True).take(20):
        print(row)
    
    # reversed_rdd.toDF(["counts","words"]).orderBy(desc("counts")).show()

except Exception as e:
    print(e)

finally:
    spark.stop()
