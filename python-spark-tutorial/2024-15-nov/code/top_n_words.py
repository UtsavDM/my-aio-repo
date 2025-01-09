from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("top_n_words") \
        .master("local[2]") \
        .getOrCreate()

sc = spark.sparkContext

in_rdd = sc.textFile("C:/Users/utsav/OneDrive/Documents/Offer Letters & official docs/Resume/more_about_me.txt")
print("============")
print(in_rdd.take(3))

in_rdd_parsed=in_rdd.map(lambda x: x.split(" "))
print(in_rdd_parsed.take(20))

in_rdd_parsed_flatten = in_rdd_parsed.flatMap(lambda x: x)
print(in_rdd_parsed_flatten.take(20))

rdd_mapped=in_rdd_parsed_flatten.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0]))
print(rdd_mapped.sortByKey(False).collect())

print("============")
spark.stop()