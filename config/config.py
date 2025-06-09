"""
Конфигурационный файл, содержащий данные для подключений к сервисам.
Предполагает использование переменных окружения при необходимости изменения.

Данные вынесены в сами скрипты, чтобы избежать проблем с импортированием.
"""

# import os

# # Elasticsearch
# ES_HOST = os.environ.get("ES_HOST", "localhost")
# ES_PORT = os.environ.get("ES_PORT", "9200")

# # Hadoop (namenode)
# # spark.hadoop.fs.defaultFS aka `hdfs://hdfs-namenode:8020`
# HADOOP_HOST = os.environ.get("HADOOP_HOST", "localhost")
# HADOOP_PORT = os.environ.get("HADOOP_PORT", "8020")

# # neo4j
# NEO4J_HOST      = os.environ.get("NEO4J_HOST", "localhost")
# NEO4J_BOLT_PORT = os.environ.get("NEO4J_BOLT_PORT", "7687")
# NEO4J_USER      = os.environ.get("NEO4J_USER", "neo4j")
# NEO4J_PASSWORD  = os.environ.get("NEO4J_PASSWORD", "password")

# # postgres
# POSTGRES_HOST      = os.environ.get("POSTGRES_HOST", "localhost")
# POSTGRES_PORT      = os.environ.get("POSTGRES_PORT", "5432")
# POSTGRES_DBNAME    = os.environ.get("POSTGRES_DBNAME", "postgres")
# POSTGRES_USER      = os.environ.get("POSTGRES_USER", "postgres")
# POSTGRES_PASSWORD  = os.environ.get("POSTGRES_PASSWORD", "iu6-magisters")