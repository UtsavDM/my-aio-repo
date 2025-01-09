import sys
from datetime import datetime
from pyspark.sql import SparkSession

job_ts=datetime.now()
job_ts=str(job_ts).replace(" ","")

spark=SparkSession.builder \
        .master("local[2]") \
        .appName("coding_practice1") \
        .getOrCreate()

sc=spark.sparkContext

input_rdd = sc.textFile("C:/Users/utsav/OneDrive/Documents/Personal/my_repo/my-aio-repo/python-spark-tutorial/2024-15-nov/dataset/dataset_for_practice1.csv")

header_row=input_rdd.take(1)
final_header_row=sc.parallelize(header_row).map(lambda x:(x.split("|")[0],x.split("|")[1],'Final Price'))
print("---")
print(final_header_row.collect())
print("---")
header_row=header_row[0]
data_only=input_rdd.filter(lambda x: x!=header_row)
# final_data_only=data_only.map(lambda x:(x[0],x[1]))

parsed_rdd=data_only.map(lambda x: x.split("|"))
# print(parsed_rdd.take(10))

# spark.stop()
# sys.exit(0)
final_data_only=parsed_rdd.map(lambda x: (x[5],x[6],int(x[5])*int(x[6])))
# print(final_data_only.take(2))
# spark.stop()
# sys.exit(0)

union_rdd=final_header_row.union(final_data_only)
print(union_rdd.collect())

def pipe_delimit(data):
    return "|".join(str(d) for d in data)

resultant_rdd=union_rdd.map(pipe_delimit)
print(resultant_rdd.collect())

# resultant_rdd.saveAsTextFile(f"file:///C:/Users/utsav/OneDrive/Documents/Personal/my_repo/my-aio-repo/python-spark-tutorial/2024-15-nov/dataset/coding_practice1_out/attempt_{job_ts}.csv")

spark.stop()
