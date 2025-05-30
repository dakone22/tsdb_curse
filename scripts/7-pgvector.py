"""
Скрипт для работы с векторными эмбеддингами в PostgreSQL с использованием pgvector.
Основные функции:
1. Получение данных о туристах из Elasticsearch
2. Создание векторных представлений с помощью модели rubert-tiny2
3. Сохранение эмбеддингов в PostgreSQL
4. Поиск ближайших соседей по векторному сходству
"""

import os
import psycopg2
from psycopg2.extras import execute_values
from elasticsearch import Elasticsearch


# Конфигурация подключения к Elasticsearch
ES_HOST = os.environ.get("ES_HOST", "localhost")  # Хост Elasticsearch
ES_PORT = os.environ.get("ES_PORT", "9200")      # Порт Elasticsearch

# Конфигурация подключения к PostgreSQL
DB_CONFIG = {
    'dbname':   os.environ.get("POSTGRES_DBNAME",   "postgres"),   # Имя БД
    'user':     os.environ.get("POSTGRES_USER",     "postgres"),   # Пользователь
    'password': os.environ.get("POSTGRES_PASSWORD", "iu6-magisters"),  # Пароль
    'host':     os.environ.get("POSTGRES_HOST",     "localhost"),  # Хост
    'port':     os.environ.get("POSTGRES_PORT",     "5432"),       # Порт
}

# Загрузка модели
from sentence_transformers import SentenceTransformer
model = SentenceTransformer("cointegrated/rubert-tiny2")
dim = model.get_sentence_embedding_dimension()
print("Model loaded")

# Подключение к Elasticsearch
es = Elasticsearch(f"http://{ES_HOST}:{ES_PORT}")
print("Connected to Elasticsearch")

# Получение документов туристов
res = es.search(index="tourists", size=1000)
docs = res['hits']['hits']
print(f"Fetched {len(docs)} documents")

# Подключение к PostgreSQL
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
print("Connected to PostgreSQL")

# Создание таблицы
cur.execute("CREATE EXTENSION IF NOT EXISTS vector")  # подключаем тип вектора!
cur.execute("DROP TABLE IF EXISTS tourist_embeddings")
cur.execute(f"CREATE TABLE IF NOT EXISTS tourist_embeddings (id SERIAL PRIMARY KEY, embedding VECTOR({dim}))")
conn.commit()
print("Table created")

# Векторизация и подготовка данных для bulk insert
data = []
for i, doc in enumerate(docs, 1):
    tourist_id = doc['_source']['id_туриста']
    text = doc['_source']['персональные_данные']  # + ' ' + doc['_source']['отзыв'] (если нужно)
    embedding = model.encode(text).tolist()
    data.append((tourist_id, embedding))

    if i % 10 == 0 or i == len(docs):
        print(f"Encoded {i}/{len(docs)} documents")

# Bulk-insert эмбеддингов
query = "INSERT INTO tourist_embeddings (id, embedding) VALUES %s"
execute_values(cur, query, data)
conn.commit()
print("Bulk insert completed")

# Поиск ближайших туристов к туристу с id=1
cur.execute("""
    SELECT id, embedding <-> (SELECT embedding FROM tourist_embeddings WHERE id = 1) AS distance
    FROM tourist_embeddings
    ORDER BY distance
    LIMIT 4
""")
results = cur.fetchall()
print("Query complete. Nearest neighbors:")

# Создаем мапу id -> данные
doc_map = {doc['_source']['id_туриста']: doc['_source']['персональные_данные'] for doc in docs}

for row in results:
    id_in_db = row[0]
    distance = row[1]
    
    # Пытаемся найти персональные данные по id
    personal_data = doc_map.get(str(id_in_db)) or doc_map.get(id_in_db) or "N/A"

    print(f"ID: {id_in_db} | Distance: {distance:.4f} | Персональные данные: {personal_data}")

# Закрытие соединения
cur.close()
conn.close()
print("Connections closed")
