import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# spark = SparkSession.builder.getOrCreate()

spark=SparkSession.builder \
        .appName("demo") \
        .master('local[2]') \
        .getOrCreate()


data = [
        (("Utsav", "", "Mehta"),["PySpark", "SQL"],"Mumbai", "India"),
        (("Dinesh", "C", "Mehta"),["SQL"],"Bangalore", "India"),
        (("Kaustubh", "", "Mehta"),["Scala"],"Delhi", "India"),
        (("Vandana", "D", "Mehta"),["Java", "C"],"NY", "US")
        ]

schema = StructType([
            StructField("name", StructType([
                StructField("first_name",StringType(),True),
                StructField("middle_name",StringType(),True),
                StructField("last_name",StringType(),True)
            ])),
            StructField("languages", ArrayType(StringType()),True),
            StructField("city",StringType(),True),
            StructField("country",StringType(),True)
])

df_init=spark.createDataFrame(data=data,schema=schema)
# df_init.printSchema()
# df_init.show(truncate=False)
df_init.filter(df_init.name.last_name.startswith("M")).select(df_init.name.first_name, df_init.country).show()
print("Stopping SparkSession")
spark.stop()