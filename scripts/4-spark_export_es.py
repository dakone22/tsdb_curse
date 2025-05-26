import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


ES_HOST = os.environ.get("ES_HOST", "localhost")
ES_PORT = os.environ.get("ES_PORT", "9200")

HADOOP_HOST = os.environ.get("HADOOP_HOST", "localhost")
HADOOP_PORT = os.environ.get("HADOOP_PORT", "8020")

HADOOP_URI = f"hdfs://{HADOOP_HOST}:{HADOOP_PORT}"


spark = SparkSession.builder \
    .appName("ExportES") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.8.0") \
    .config("es.nodes", f"{ES_HOST}:{ES_PORT}") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", f"{ES_PORT}") \
    .getOrCreate()

# Читаем индексы
tourists_df = spark.read.format("es") \
    .option("es.resource", "tourists") \
    .load()

tours_df = spark.read.format("es") \
    .option("es.resource", "tours") \
    .load()

# Разделяем покупки из tourists_df
purchases_df = tourists_df.select("id_туриста", "id_тура", "дата_тура")
tourists_table = tourists_df.select("id_туриста", "персональные_данные")
tours_table = tours_df.select("id_тура", "название", "страна", "стоимость")

# Сохраняем в HDFS
tourists_table.write.mode("overwrite").option("header", True).csv(f"{HADOOP_URI}/data/tourists")
tours_table.write.mode("overwrite").option("header", True).csv(f"{HADOOP_URI}/data/tours")
purchases_df.write.mode("overwrite").option("header", True).csv(f"{HADOOP_URI}/data/purchases")

# Сохраняем локально (для Neo4j)
tourists_table.coalesce(1).write.mode("overwrite").option("header", True).csv("output/tourists")
tours_table.coalesce(1).write.mode("overwrite").option("header", True).csv("output/tours")
purchases_df.coalesce(1).write.mode("overwrite").option("header", True).csv("output/purchases")

def move_single_csv(src_dir, output_filename):
    for fname in os.listdir(src_dir):
        if fname.startswith("part-") and fname.endswith(".csv"):
            os.rename(
                os.path.join(src_dir, fname),
                os.path.join("output", output_filename)
            )
            break

move_single_csv("output/tourists", "tourists.csv")
move_single_csv("output/tours", "tours.csv")
move_single_csv("output/purchases", "purchases.csv")