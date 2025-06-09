"""
Альтернатива скрипту `scripts/6-spark_select.py`.
Данные читаются не из ES, а из hdfs.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Настройки подключения к Hadoop
HOST = os.environ.get("HOST", "localhost")   # Хост Hadoop
PORT = os.environ.get("PORT", "8020")        # Порт Hadoop

# Формирование URI для подключения к HDFS
HADOOP_URI = f"hdfs://{HOST}:{PORT}"

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
