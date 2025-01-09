from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local[4]") \
                    .appName("IPL dataset") \
                    .getOrCreate()

path="C:/Users/utsav/OneDrive/Documents/Personal/my_repo/my-aio-repo/python-spark-tutorial/2024-15-nov/dataset"
input_file="matches.csv"

matches_rdd=spark._sc.textFile(f"{path}/{input_file}")

header=matches_rdd.take(1)

only_data=matches_rdd.filter(lambda x: x != header)

city_rdd=only_data.map(lambda x: x.split(",")[2])
player_rdd= only_data.map(lambda x: x.split(",")[5])

clean_city=city_rdd.filter(lambda x: x!="" or x!=" ")
clean_player=player_rdd.filter(lambda x: x!="" or x!=" ")

city_mapper=clean_city.map(lambda x: (x,1))
player_mapper=clean_player.map(lambda x: (x,1))

city_reduced=city_mapper.reduceByKey(lambda x,y: x+y)
player_reduced=player_mapper.reduceByKey(lambda x,y: x+y)

city_reverse_rdd=city_reduced.map(lambda x: (x[1],x[0]))
player_reverse_rdd=player_reduced.map(lambda x: (x[1],x[0]))

print(city_reverse_rdd.sortByKey(False).take(1))
print(player_reverse_rdd.sortByKey(False).take(1))
