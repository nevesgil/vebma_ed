from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialize SparkSession
master = "spark://spark-master:7077"
conf = SparkConf().setAppName("Countries Broadcast Join").setMaster(master)
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Create a large DataFrame with countries data
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

# Create a smaller DataFrame with more columns and repeated entries
small_df = spark.createDataFrame([
    ("Brazil", "South America", "Brasília", "Portuguese"),
    ("Argentina", "South America", "Buenos Aires", "Spanish"),
    ("South Africa", "Africa", "Pretoria", "Afrikaans"),
    ("Brazil", "South America", "Brasília", "Portuguese"),
    ("Argentina", "South America", "Buenos Aires", "Spanish")
], ["country", "continent", "capital", "language"])


# Count the number of records in the joined DataFrame
count = small_df.count()

big_df.show()

print("###############################################################################################################################################################################################################################", count)

small_df.show()

# Print the count and a success message
print(f"Record count in the joined DataFrame: {count}")

# Stop the Spark session
spark.stop()
