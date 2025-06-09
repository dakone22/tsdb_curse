import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Настройки подключения к Hadoop
HOST = os.environ.get("HOST", "localhost")   # Хост Hadoop
PORT = os.environ.get("PORT", "8020")        # Порт Hadoop

# Формирование URI для подключения к HDFS
HADOOP_URI = f"hdfs://{HOST}:{PORT}"

# Создаем SparkSession без подключения к Elasticsearch
spark = SparkSession.builder \
    .appName("ReadFromHDFS") \
    .getOrCreate()

# ----------------------------------------------------------------------
# Читаем данные из HDFS (CSV-файлы), предварительно записанные ранее:
#
# - Каталог HDFS: /data/tourists  (файлы CSV с данными туристов)
# - Каталог HDFS: /data/tours     (файлы CSV с данными туров)
#
# Предполагается, что в этих каталогах лежат CSV с заголовками, соответствующие структурам:
#   tourists: id_туриста, персοнальные_данные, id_тура, дата_тура
#   tours:    id_тура, название, страна, стоимость
# ----------------------------------------------------------------------

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

# Если у вас в CSV-хранилище разделены таблицы (tourists_table без id_тура и purchases.csv),
# тогда вместо объединения tourists_df и tours_df сразу, нужно:
#   purchases_df = spark.read.option("header", True).csv(f"{HADOOP_URI}/data/purchases")
#   tours_df     = spark.read.option("header", True).csv(f"{HADOOP_URI}/data/tours")
#   result_df = purchases_df.join(tours_df, on="id_тура") \
#                           .groupBy("название") \
#                           .agg(count("*").alias("число_покупок")) \
#                           .orderBy(col("число_покупок").desc())
#
# Однако, если ваш tourists.csv уже содержит поля id_тура (как это было при выгрузке из ES),
# можно выполнять объединение напрямую. Ниже приведен вариант, аналогичный «Варианту 2» из исходного скрипта.

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
