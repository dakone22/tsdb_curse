# Это нужно запускать интерактивно!
import os

from pyspark.sql import SparkSession

HADOOP_HOST = os.environ.get("HADOOP_HOST", "localhost")
HADOOP_PORT = os.environ.get("HADOOP_PORT", "8020")

# создание сессии
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
# чтобы уменьшить число сообщений INFO
spark.sparkContext.setLogLevel("WARN")

# создать DataFrame из csv-файла с внутренней схемой
data = spark.read.load(f"hdfs://{HADOOP_HOST}:{HADOOP_PORT}/data/tours", format="csv", sep=",", inferSchema="true", header="true")
# зарегистрировать DataFrame как временную таблицу temp
data.createOrReplaceTempView("temp")
# выполнить оператор select и показать результаты
df = spark.sql("select * from temp").show()

ui_url = spark.sparkContext.uiWebUrl
print(f"Visit {ui_url} or try localhost")

# ...

spark.stop()