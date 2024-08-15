from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

master = "spark://spark-master:7077"
conf = SparkConf().setAppName("Countries Broadcast Join").setMaster(master)
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

big_df = spark.createDataFrame([
    ("USA", "North America", 331002651, "Washington, D.C.", "English"),
    ("Brazil", "South America", 212559417, "Brasília", "Portuguese"),
    ("Argentina", "South America", 45195777, "Buenos Aires", "Spanish"),
    ("China", "Asia", 1439323776, "Beijing", "Mandarin"),
    ("India", "Asia", 1380004385, "New Delhi", "Hindi"),
    ("Germany", "Europe", 83783942, "Berlin", "German"),
    ("UK", "Europe", 67886011, "London", "English"),
    ("South Africa", "Africa", 59308690, "Pretoria", "Afrikaans"),
    ("Egypt", "Africa", 91250000, "Cairo", "Arabic"),
    ("Brazil", "South America", 212559417, "Brasília", "Portuguese"),
    ("Argentina", "South America", 45195777, "Buenos Aires", "Spanish"),
    ("China", "Asia", 1439323776, "Beijing", "Mandarin"),
    ("India", "Asia", 1380004385, "New Delhi", "Hindi"),
    ("Germany", "Europe", 83783942, "Berlin", "German"),
    ("UK", "Europe", 67886011, "London", "English"),
    ("South Africa", "Africa", 59308690, "Pretoria", "Afrikaans"),
    ("Egypt", "Africa", 91250000, "Cairo", "Arabic")
], ["country", "continent", "population", "capital", "language"])

small_df = spark.createDataFrame([
    ("Brazil", "South America", "Brasília", "Portuguese"),
    ("Argentina", "South America", "Buenos Aires", "Spanish"),
    ("South Africa", "Africa", "Pretoria", "Afrikaans"),
    ("Brazil", "South America", "Brasília", "Portuguese"),
    ("Argentina", "South America", "Buenos Aires", "Spanish")
], ["country", "continent", "capital", "language"])


count = small_df.count()

big_df.show()

print("###############################################################################################################################################################################################################################", count)

small_df.show()

print(f"count DataFrame: {count}")

spark.stop()
