### Обзор решения для варианта 20 (Турфирма)

**Цель проекта:** Построить сквозной конвейер данных для туристической фирмы, включающий подготовку данных, их индексирование и анализ в Elasticsearch/Kibana, хранение в графовой БД Neo4j, обработку в Spark/HDFS и семантический поиск с Pgvector.

---

## Шаг 1. Структура проекта и Docker Compose

Создать каталог `tour-agency-project/` со следующей структурой:

```
tour-agency-project/
├── config
│   └── config.py
├── data
│   ├── tourists
│   └── tours
├── docker-compose.yml
├── es
│   ├── config
│   │   ├── jvm.options
│   │   └── log4j2.properties
│   └── queries
│       ├── tourists_aggregation.json
│       └── tours_aggregation.json
├── neo4j
│   └── queries
│       ├── import_to_neo4j.cypher
│       ├── popular_tours.cypher
│       └── show_all.cypher
├── requirements.txt
└── scripts
    ├── 0-generate_data.py
    ├── 1-es_create_index.py
    ├── 2-es_load.py
    ├── 3-es_queries.py
    ├── 4-spark_export_es.py
    ├── 5-neo4j_queries.py
    ├── 6-spark_select.py
    ├── 7-pgvector.py
    └── run_cypher.py
```

**docker-compose.yml**

```yaml
version: "3.8"
services:
  elasticsearch:
    image: elasticsearch:8.8.0
    environment:
      - discovery.type=single-node
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
      - network.host=0.0.0.0
      - xpack.security.enabled=false           # https выкл
      - xpack.security.http.ssl.enabled=false  # https выкл
    ports:
      - "9200:9200"
    volumes:
      - ./es/config:/usr/share/elasticsearch/config
      - es-data:/usr/share/elasticsearch/data
    networks:
      - tour-net

  kibana:
    image: kibana:8.8.0
    environment:
      ELASTICSEARCH_HOSTS: http://elasticsearch:9200
    ports:
      - "5601:5601"
    depends_on:
      - elasticsearch
    networks:
      - tour-net

  neo4j:
    image: neo4j:5.11
    environment:
      NEO4J_AUTH: "neo4j/password"
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - neo4j-data:/data
      - ./output:/import  # для возможности загрузить csv-данные
    networks:
      - tour-net

  hdfs-namenode:
    image: cicorias/hadoop-namenode:3.2.1
    environment:
      - CLUSTER_NAME=tour-cluster
      - CORE_CONF_fs_defaultFS=hdfs://hdfs-namenode:8020
      - HDFS_CONF_dfs_replication=1
      - HDFS_CONF_dfs_permissions_enabled=false  # отключаем проверку доступов пользователей
    ports:
      - "9870:9870"
      - "8020:8020" # так как запускаем всё внутри сети контейнеров, вытаскиваем наружу порты, и придётся юзать hdfs://localhost:8020, так как hdfs://`hdfs-namenode`:8020 снаружи мы не достанем
    volumes:
      - hdfs-nn-data:/hadoop/dfs/name
    networks:
      - tour-net

  hdfs-datanode:
    image: cicorias/hadoop-datanode:3.2.1
    environment:
      - CORE_CONF_fs_defaultFS=hdfs://hdfs-namenode:8020
      - HDFS_CONF_dfs_replication=1
      - HDFS_CONF_dfs_permissions_enabled=false
    depends_on:
      - hdfs-namenode
    volumes:
      - hdfs-dn-data:/hadoop/dfs/data
    networks:
      - tour-net

  spark-master:
    image: bitnami/spark:3.2.1
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
    ports:
      - "8080:8080"
      - "7077:7077"
    depends_on:
      - hdfs-namenode
      - elasticsearch
    networks:
      - tour-net

  spark-worker:
    image: bitnami/spark:3.2.1
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
    depends_on:
      - spark-master
    networks:
      - tour-net

  postgres:
    image: pgvector/pgvector:pg15
    environment:
      POSTGRES_PASSWORD: iu6-magisters
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    networks:
      - tour-net

volumes:
  es-data:
  neo4j-data:
  hdfs-nn-data:
  hdfs-dn-data:
  pgdata:

networks:
  tour-net:
    driver: bridge
```

`docker-compose down -v` - выключить и удалить volumes (флаг -v)
`docker-compose up -d` - поднять всё

---

## Шаг 2. Генерация JSON-документов

Скрипт `scripts/0-generate_data.py` генерирует по 30 документов каждого типа в `data/tours/` и `data/tourists/`:

```python
import os, json, random
from datetime import date, timedelta

# Настройки
num_tours = 30
num_tourists = 30
start_date = date(2020, 1, 1)

# Данные
real_countries = ["Россия", "Италия", "Испания", "Франция", "Таиланд"]
funny_places = ["в ад", "на Луну", "в Бобруйск", "на дачу к теще", "в офис", "в трясину", "в Зазеркалье"]
real_places = ["Барселону", "Милан", "Бангкок", "Париж", "Сочи", "Рим", "Прагу", "Амстердам"]

funny_adjectives = ["Последнее", "Безумное", "Забытое", "Бессмысленное", "Потустороннее", "Потное"]
real_adjectives = ["Увлекательное", "Экзотическое", "Романтическое", "Приключенческое", "Историческое", "Расслабляющее"]
trip_types = ["путешествие", "тур", "поездка"]

first_names = ["Алексей", "Мария", "Иван", "Ольга", "Сергей", "Анна", "Дмитрий", "Елена", "Николай", "Татьяна"]
last_names = ["Иванов", "Смирнова", "Кузнецов", "Попова", "Соколов", "Морозова", "Лебедев", "Козлова", "Новиков", "Федорова"]

positive_reviews = ["Отличный тур, рекомендую!", "Все понравилось!", "Незабываемое путешествие!", "Очень доволен поездкой."]
neutral_reviews = ["В целом нормально.", "Было интересно, но ожидал большего.", "Средний тур."]
negative_reviews = ["Разочарован, не оправдало ожиданий.", "Сервис оставляет желать лучшего.", "Больше не поеду."]

def generate_description():
    intro_phrases = [
        "Для любителей острых ощущений",
        "Идеально подходит тем, кто устал от обычного",
        "Уникальный шанс испытать нечто новое",
        "Для тех, кто хочет убежать от рутины",
        "Подарите себе незабываемые эмоции",
        "Предложение для настоящих романтиков",
        "Когда отпуск зовёт приключения",
    ]

    actions = [
        "экскурсия по",
        "плавание в",
        "пеший поход через",
        "медитация среди",
        "ночёвка на вершине",
        "спонтанный квест в",
        "прогулка с гидами по",
        "неожиданные встречи в",
    ]

    locations_real = [
        "древним улочкам Рима",
        "живописным долинам Франции",
        "пляжам Таиланда",
        "набережной Барселоны",
        "лесам Карелии",
        "улицам старого города",
    ]

    locations_funny = [
        "переулкам Зазеркалья",
        "заброшенным пивоварням",
        "котокафе на краю вселенной",
        "мысу Отчаяния",
        "тёмным лесам бюрократии",
        "кладбищу надежд и Wi-Fi",
    ]

    endings = [
        "Включено: трансфер, питание и пара экзистенциальных кризисов.",
        "Никакой скуки — только вы, природа и немного паники.",
        "Гарантируем: вернётесь другим человеком. Возможно.",
        "Без лишних слов — просто берите билет.",
        "Подходит даже тем, кто боится приключений.",
        "Берите зонт. Или зелье от страха.",
    ]

    # Случайно миксуем реальные и юмористические локации
    location = random.choice(locations_real + locations_funny)

    return f"{random.choice(intro_phrases)}, {random.choice(actions)} {location}. {random.choice(endings)}"


# Папки
os.makedirs('data/tours', exist_ok=True)
os.makedirs('data/tourists', exist_ok=True)

# Генерация туров
for i in range(1,num_tours+1):
    adj = random.choice(real_adjectives + funny_adjectives)
    place = random.choice(real_places + funny_places)
    name = f"{adj} {random.choice(trip_types)} {place}"
    tour = {
        'id_тура': i,
        'название': name,
        'страна': random.choice(real_countries),
        'описание': generate_description(),
        'стоимость': round(random.uniform(50000, 200000), 2),
        'услуга': random.sample(["экскурсия", "трансфер", "питание", "страховка"], random.randint(1, 4))
    }
    with open(f"data/tours/tour_{i}.json", 'w', encoding='utf-8') as f:
        json.dump(tour, f, ensure_ascii=False)

# Генерация туристов и покупок
for i in range(1,num_tourists+1):
    fio = f"{random.choice(last_names)} {random.choice(first_names)}"
    purchase_date = start_date + timedelta(days=random.randint(0, 1000))
    tour_id = random.randint(1, num_tours)
    review_pool = random.choices([positive_reviews, neutral_reviews, negative_reviews], weights=[0.6, 0.25, 0.15])[0]
    review = random.choice(review_pool)

    tourist = {
        'id_туриста': i,
        'персональные_данные': fio,
        'дата_тура': purchase_date.isoformat(),
        'id_тура': tour_id,
        'страна': random.choice(real_countries),
        'сведения_о_визе': "Тип визы и дата выдачи.",
        'отзыв': review
    }
    with open(f"data/tourists/tourist_{i}.json", 'w', encoding='utf-8') as f:
        json.dump(tourist, f, ensure_ascii=False)
```

---

## Шаг 3. Elasticsearch: анализатор, маппинг, индексация, агрегации

1. **Конфигурация ES** (`es/config/...`):

Необходимо создать минимальные конфиг-файлы для запуска: `jvm.options` и `log4j2.properties`
 
2. **Маппинг** (`scripts/1-es_create_index.py`): создаём один индекс `tour_agency` с типами `tour` и `tourist`:

```python
import os

from elasticsearch import Elasticsearch, exceptions

ES_HOST = os.environ.get("ES_HOST", "localhost")
ES_PORT = os.environ.get("ES_PORT", "9200")

es = Elasticsearch(f"http://{ES_HOST}:{ES_PORT}")

# Общие настройки анализатора
analysis_settings = {"analysis":{"filter":{"russian_stop":{"type":"stop","stopwords":"_russian_"},"russian_snowball":{"type":"snowball","language":"Russian"}},"analyzer":{"russian_custom":{"type":"custom","tokenizer":"standard","filter":["lowercase","russian_stop","russian_snowball"]}}}}

# 1) Создаём индекс «tourists»
tourists_body = {
    "settings": analysis_settings,
    "mappings": {
        "properties": {
            "id_туриста":           { "type": "keyword" },
            "персональные_данные":  { "type": "text", "analyzer": "russian_custom" },
            "дата_тура":            { "type": "date", "format": "yyyy-MM-dd" },
            "id_тура":              { "type": "keyword" },
            "страна":               { "type": "keyword" },
            "сведения_о_визе":      { "type": "text", "analyzer": "russian_custom" },
            "отзыв":                { "type": "text", "analyzer": "russian_custom" }
        }
    }
}

# 2) Создаём индекс «tours»
tours_body = {
    "settings": analysis_settings,
    "mappings": {
        "properties": {
            "название":  { "type": "keyword" },
            "страна":    { "type": "keyword" },
            "описание":  { "type": "text", "analyzer": "russian_custom" },
            "стоимость": { "type": "double" },
            "услуга":    { "type": "text", "analyzer": "russian_custom" }
        }
    }
}

def create_index(name, body):
    try:
        if es.indices.exists(index=name):
            print(f"Индекс '{name}' уже существует, удаляем...")
            es.indices.delete(index=name)
        print(f"Создаём индекс '{name}'...")
        es.indices.create(index=name, body=body)
        print(f"Индекс '{name}' успешно создан.")
    except exceptions.ElasticsearchException as e:
        print(f"Ошибка при создании индекса '{name}':", e)

if __name__ == "__main__":
    create_index("tourists", tourists_body)
    create_index("tours", tours_body)
```

Затем загружаем данные:
`scripts/2-es_load.py`:

```py
from elasticsearch import Elasticsearch
import json
import os

ES_HOST = os.environ.get("ES_HOST", "localhost")
ES_PORT = os.environ.get("ES_PORT", "9200")

es = Elasticsearch(f"http://{ES_HOST}:{ES_PORT}")

# Загрузка данных туристов
tourist_dir = "data/tourists"
for filename in os.listdir(tourist_dir):
    with open(os.path.join(tourist_dir, filename), 'r') as f:
        tourist = json.load(f)
        es.index(index="tourists", id=tourist["id_туриста"], document=tourist)

# Загрузка данных туров
tour_dir = "data/tours"
for filename in os.listdir(tour_dir):
    with open(os.path.join(tour_dir, filename), 'r') as f:
        tour = json.load(f)
        es.index(index="tours", id=tour["id_тура"], document=tour)

print("Данные успешно загружены в Elasticsearch")

print("\nПример данных из индекса 'tourists':")
try:
    tourist_example = es.search(index="tourists", size=1)
    print(json.dumps(tourist_example["hits"]["hits"][0]["_source"], indent=2, ensure_ascii=False))
except Exception as e:
    print("Ошибка при получении данных из индекса 'tourists':", e)

print("\nПример данных из индекса 'tours':")
try:
    tour_example = es.search(index="tours", size=1)
    print(json.dumps(tour_example["hits"]["hits"][0]["_source"], indent=2, ensure_ascii=False))
except Exception as e:
    print("Ошибка при получении данных из индекса 'tours':", e)

analyzed = es.indices.analyze(
    index="tourists",
    body={
        "analyzer": "russian_custom",  # название твоего анализатора
        "text": "Съешь ещё этих мягких французских булок, да выпей чаю"
    }
)

print("\nРезультат анализа текста:")
for token in analyzed["tokens"]:
    print(token["token"])
```

3. **Агрегации**:
   `es/queries/tourists_aggregation.json`
   ```json
   {
      "size": 0,
      "aggs": {
        "tours_by_year": {
          "date_histogram": {
            "field":             "дата_тура",
            "calendar_interval": "year"
          },
          "aggs": {
            "sales_by_country": {
              "terms": {
                "field": "страна",
                "size": 20
              }
            }
          }
        }
      }
    }
   ```
    * `date_histogram` с `calendar_interval: "year"` разбивает все документы по годам даты тура.
    * Внутри каждой «бочки» года вложенная `terms`-агрегация по полю `страна` подсчитывает число документов (проданных туров) в каждой стране.

   
   `es/queries/tours_aggregation.json`
   ```json
    {
      "size": 0,
      "aggs": {
        "tours_per_country": {
          "terms": {
            "field": "страна",
            "size": 20
          }
        }
      }
    }
   ```

    * Здесь простая `terms`-агрегация по полю `страна`, которая вернёт список стран и число туров в каждой из них.

   ```bash
   curl -X POST "http://localhost:9200/tourists/_search" -H "Content-Type: application/json" -d @es/queries/tourist_aggregation.json
   curl -X POST "http://localhost:9200/tours/_search" -H "Content-Type: application/json" -d @es/queries/tour_aggregation.json
   ```
   или скриптом `scripts/3-es_queries.py`:
```py
import subprocess

# Базовая команда
runner = ['curl', '-H', 'Content-Type: application/json']

# Список команд с указанием URL и файла запроса
commands = [
    ['-X', 'POST', 'http://localhost:9200/tours/_search', '-d', '@es/queries/tours_aggregation.json'],
    ['-X', 'POST', 'http://localhost:9200/tourists/_search', '-d', '@es/queries/tourists_aggregation.json'],
]

# Запуск каждой команды
for cmd_args in commands:
    cmd = runner + cmd_args
    print(f"Запуск: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    print("\n"+"="*10)
```
   
4. В Kibana на основе агрегаций создаются визуализации:

   * Гистограмма продаж по годам с разбивкой по странам.
   * Таблица общего числа туров по странам.

---

## Шаг 4. Neo4j: импорт и запрос

1. Экспорт трёх CSV из Elasticsearch (для удобства это происходит в скрипте `scripts/4-spark_export_es.py`):

   * `tours.csv`: поля `id_тура, название, страна, стоимость`
   * `tourists.csv`: поля `id_туриста, персональные_данные_туриста`
   * `purchases.csv`: поля `id_туриста, id_тура, дата_тура`
2. Сценарий `neo4j/queries/import_to_neo4j.cypher`:

```cypher
CALL {
  LOAD CSV WITH HEADERS FROM 'file:///tours.csv' AS row
  MERGE (tr:Tour {id: row.id_тура})
    SET tr.name    = row.название,
        tr.country = row.страна,
        tr.price   = toFloat(row.стоимость)
} IN TRANSACTIONS OF 500 ROWS;

CALL {
  LOAD CSV WITH HEADERS FROM 'file:///tourists.csv' AS row
  MERGE (t:Tourist {id: row.id_туриста})
    SET t.name = row.персональные_данные_туриста
} IN TRANSACTIONS OF 500 ROWS;

CALL {
  LOAD CSV WITH HEADERS FROM 'file:///purchases.csv' AS row
  MATCH (t:Tourist {id: row.id_туриста})
  MATCH (tr:Tour    {id: row.id_тура})
  MERGE (t)-[:BOUGHT {date: row.дата_тура}]->(tr)
} IN TRANSACTIONS OF 500 ROWS;
```
3. **Cypher-запрос** для самых популярных туров (`neo4j/queries/popular_tours.cypher`):

```cypher
MATCH (t:Tourist)-[b:BOUGHT]->(tr:Tour)
RETURN tr.name AS tour, count(b) AS sales
ORDER BY sales DESC
LIMIT 5;
```

Запуск обоих сценариев через `5-neo4j_queries.py`:
```py
import subprocess

runner = ['python', 'scripts/run_cypher.py']
cypher_files = [
    'neo4j/queries/import_to_neo4j.cypher',
    'neo4j/queries/popular_tours.cypher',
]

for file in cypher_files:
    cmd = runner + [file]
    print(f"Запуск: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

print("You can visit http://localhost:7474/browser/ or something...")
```

---

## Шаг 5. Spark + HDFS

1. Скрипт `scripts/4-spark_export_es.py` (Spark + ES-Hadoop):

```python
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
tourists_df = spark.read.format("es").option("es.resource", "tourists").load()
tours_df = spark.read.format("es").option("es.resource", "tours").load()

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
```
   Для использования Spark 3.2.1+ можно и нужно понизиться хотя бы до Java 17!
2. **Spark SQL**: запрос числа проданных туров по названию:

```sql
SELECT t.название, COUNT(p.id_тура) AS sold_count
FROM purchases p
JOIN tours t ON p.id_тура = t.id_тура
GROUP BY t.название;
```
через `6-spark_select.py`:
```py
# ... аналогичный код из начала 4-spark_export_es.py до `tourists_df` ...

from pyspark.sql.functions import col, count
result_df = tourists_df.join(tours_df, on="id_тура") \
    .groupBy("название") \
    .agg(count("*").alias("число_покупок")) \
    .orderBy(col("число_покупок").desc())

result_df.show()

input(f"visit http://localhost:4040 or {spark.sparkContext.uiWebUrl}...")
```
3. анализ временной диаграммы в Web UI (порт 4040).

---

## Шаг 6. Pgvector: векторизация и поиск

1. Скрипт `scripts/7-pgvector.py`:

```python
import os
import psycopg2
from psycopg2.extras import execute_values
from elasticsearch import Elasticsearch

# Конфигурация подключения
ES_HOST = os.environ.get("ES_HOST", "localhost")
ES_PORT = os.environ.get("ES_PORT", "9200")

DB_CONFIG = {
    'dbname':    os.environ.get("POSTGRES_DBNAME",   "postgres"),
    'user':      os.environ.get("POSTGRES_USER",     "postgres"),
    'password':  os.environ.get("POSTGRES_PASSWORD", "iu6-magisters"),
    'host':      os.environ.get("POSTGRES_HOST",     "localhost"),
    'port':      os.environ.get("POSTGRES_PORT",     "5432"),
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
```
2. **SQL-запрос** для трёх ближайших соседей:

   ```sql
   SELECT id, vector <-> (SELECT vector FROM tourist_vectors WHERE id = '<target_id>') AS distance
   FROM tourist_vectors
   ORDER BY distance
   LIMIT 3;
   ```
