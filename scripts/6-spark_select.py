import os

from pyspark.sql import SparkSession


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
tourists_df = spark.read.format("es").option("es.resource", "tourists").load()
tours_df = spark.read.format("es").option("es.resource", "tours").load()

# Вариант 1
# tourists_df_clean = tourists_df \
#     .withColumnRenamed("id_туриста", "tourist_id") \
#     .withColumnRenamed("персональные_данные", "tourist_name") \
#     .withColumnRenamed("дата_тура", "tour_date") \
#     .withColumnRenamed("id_тура", "tour_id")

# tours_df_clean = tours_df \
#     .withColumnRenamed("id_тура", "tour_id") \
#     .withColumnRenamed("название", "name") \
#     .withColumnRenamed("страна", "country") \
#     .withColumnRenamed("стоимость", "price")

# tourists_df_clean.createOrReplaceTempView("tourists")
# tours_df_clean.createOrReplaceTempView("tours")

# result_df = spark.sql("""
#     SELECT t.name, COUNT(*) AS sales_count
#     FROM tourists AS tr, tours AS t
#     WHERE tr.tour_id = t.tour_id
#     GROUP BY t.name
#     ORDER BY sales_count DESC
# """)

# result_df.show()
# result_df.explain(mode="formatted")

# Вариант 2
from pyspark.sql.functions import col, count
result_df = tourists_df.join(tours_df, on="id_тура") \
    .groupBy("название") \
    .agg(count("*").alias("число_покупок")) \
    .orderBy(col("число_покупок").desc())

result_df.show()


input(f"visit http://localhost:4040 or {spark.sparkContext.uiWebUrl}...")