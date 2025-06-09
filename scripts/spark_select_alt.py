"""
Альтернатива скрипту `scripts/6-spark_select.py`.
Данные читаются не из ES, а из hdfs.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Настройки подключения к Hadoop
HADOOP_HOST = os.environ.get("HADOOP_HOST", "localhost")
HADOOP_PORT = os.environ.get("HADOOP_PORT", "8020")

# Формирование URL для подключения к HDFS
HADOOP_URI = f"hdfs://{HADOOP_HOST}:{HADOOP_PORT}"

# Создаем SparkSession
spark = SparkSession.builder \
    .appName("ReadFromHDFS") \
    .getOrCreate()

tourists_df = (
    spark.read
         .option("header", True)
         .csv(f"{HADOOP_URI}/data/tourists")
)

tours_df = (
    spark.read
         .option("header", True)
         .csv(f"{HADOOP_URI}/data/tours")
)

result_df = (
    tourists_df
        .join(tours_df, on="id_тура")
        .groupBy("название")
        .agg(count("*").alias("число_покупок"))
        .orderBy(col("число_покупок").desc())
)

# Выводим результат в консоль
result_df.show()

# Пауза, чтобы можно было посетить SparkUI
input(f"Visit SparkUI at http://localhost:4040 or {spark.sparkContext.uiWebUrl} and press Enter to exit...")
