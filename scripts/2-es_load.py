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