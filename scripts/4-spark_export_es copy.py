"""
Скрипт для экспорта данных из Elasticsearch в HDFS и локальную файловую систему.
Читает данные из индексов 'tourists' и 'tours', преобразует их и сохраняет:
- В HDFS для дальнейшей обработки в Hadoop
- Локально в формате CSV для загрузки в Neo4j
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Настройки подключения к Elasticsearch
ES_HOST = os.environ.get("ES_HOST", "localhost")  # Хост Elasticsearch
ES_PORT = os.environ.get("ES_PORT", "9200")      # Порт Elasticsearch

# Настройки подключения к Hadoop
HADOOP_HOST = os.environ.get("HADOOP_HOST", "localhost")  # Хост Hadoop
HADOOP_PORT = os.environ.get("HADOOP_PORT", "8020")      # Порт Hadoop
HADOOP_URI = f"hdfs://{HADOOP_HOST}:{HADOOP_PORT}"       # URI для подключения к HDFS

# Создаем SparkSession с настройками для работы с Elasticsearch
# - Имя приложения Spark
# - spark.jars.packages: Подключаем Elasticsearch connector
# - es.nodes:            Адрес Elasticsearch
# - spark.es.nodes:      Имя сервиса Elasticsearch в Docker
# - spark.es.port:       Порт Elasticsearch
spark = SparkSession.builder \
    .appName("ExportES") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.8.0") \
    .config("es.nodes", f"{ES_HOST}:{ES_PORT}") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", f"{ES_PORT}") \
    .getOrCreate()

# Чтение данных из индекса 'tourists' в Elasticsearch
# - es.resource: Указываем имя индекса
tourists_df = spark.read.format("es") \
    .option("es.resource", "tourists") \
    .load()

# Чтение данных из индекса 'tours' в Elasticsearch
# - es.resource: Указываем имя индекса
tours_df = spark.read.format("es") \
    .option("es.resource", "tours") \
    .load()

# Разделяем данные о туристах на три таблицы:
# 1. Таблица покупок (связь турист-тур)
purchases_df = tourists_df.select("id_туриста", "id_тура", "дата_тура")
# 2. Таблица туристов (основная информация)
tourists_table = tourists_df.select("id_туриста", "персональные_данные", "id_тура")
# 3. Таблица туров (основная информация)
tours_table = tours_df.select("id_тура", "название", "страна", "стоимость")

# Сохраняем данные в HDFS в формате CSV
tourists_table.write.mode("overwrite").option("header", True).csv(f"{HADOOP_URI}/data/tourists")
tours_table.write.mode("overwrite").option("header", True).csv(f"{HADOOP_URI}/data/tours")
purchases_df.write.mode("overwrite").option("header", True).csv(f"{HADOOP_URI}/data/purchases")

# Сохраняем данные локально в одну CSV (для загрузки в Neo4j)
tourists_table.coalesce(1).write.mode("overwrite").option("header", True).csv("output/tourists")
tours_table.coalesce(1).write.mode("overwrite").option("header", True).csv("output/tours")
purchases_df.coalesce(1).write.mode("overwrite").option("header", True).csv("output/purchases")

def move_single_csv(src_dir, output_filename):
    """
    Переносит сгенерированный Spark CSV-файл в указанное место.
    Spark создает несколько part-файлов, нам нужен только один.
    
    Args:
        src_dir (str): Директория с файлами Spark
        output_filename (str): Имя итогового файла
    """
    for fname in os.listdir(src_dir):
        if fname.startswith("part-") and fname.endswith(".csv"):
            # Переименовываем part-файл в итоговый CSV
            os.rename(
                os.path.join(src_dir, fname),
                os.path.join("output", output_filename)
            )
            break

# Переносим CSV-файлы в корень output
move_single_csv("output/tourists", "tourists.csv")
move_single_csv("output/tours", "tours.csv")
move_single_csv("output/purchases", "purchases.csv")
