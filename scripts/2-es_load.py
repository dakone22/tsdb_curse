"""
Скрипт для загрузки данных о турах и туристах в Elasticsearch.
Читает JSON-файлы из папок data/tours и data/tourists и загружает их в соответствующие индексы.
"""

from elasticsearch import Elasticsearch
import json
import os

# Настройки подключения к Elasticsearch
ES_HOST = os.environ.get("ES_HOST", "localhost")  # Хост Elasticsearch
ES_PORT = os.environ.get("ES_PORT", "9200")      # Порт Elasticsearch

# Инициализация клиента Elasticsearch
es = Elasticsearch(f"http://{ES_HOST}:{ES_PORT}")

# Загрузка данных о туристах
tourist_dir = "data/tourists"
print(f"Загрузка данных туристов из {tourist_dir}...")
for filename in os.listdir(tourist_dir):
    # Читаем JSON-файл с данными туриста
    with open(os.path.join(tourist_dir, filename), 'r') as f:
        tourist = json.load(f)
        # Загружаем документ в индекс 'tourists'
        # Используем id_туриста в качестве идентификатора документа
        es.index(index="tourists", id=tourist["id_туриста"], document=tourist)

# Загрузка данных о турах
tour_dir = "data/tours"
print(f"Загрузка данных туров из {tour_dir}...")
for filename in os.listdir(tour_dir):
    # Читаем JSON-файл с данными тура
    with open(os.path.join(tour_dir, filename), 'r') as f:
        tour = json.load(f)
        # Загружаем документ в индекс 'tours'
        # Используем id_тура в качестве идентификатора документа
        es.index(index="tours", id=tour["id_тура"], document=tour)

print("Данные успешно загружены в Elasticsearch")


# Проверка загруженных данных - выводим по одному документу из каждого индекса
print("\nПример данных из индекса 'tourists':")
try:
    # Получаем один случайный документ из индекса 'tourists'
    tourist_example = es.search(index="tourists", size=1)
    # Красиво форматируем и выводим документ
    print(json.dumps(tourist_example["hits"]["hits"][0]["_source"], indent=2, ensure_ascii=False))
except Exception as e:
    print("Ошибка при получении данных из индекса 'tourists':", e)

print("\nПример данных из индекса 'tours':")
try:
    # Получаем один случайный документ из индекса 'tours'
    tour_example = es.search(index="tours", size=1)
    # Красиво форматируем и выводим документ
    print(json.dumps(tour_example["hits"]["hits"][0]["_source"], indent=2, ensure_ascii=False))
except Exception as e:
    print("Ошибка при получении данных из индекса 'tours':", e)

# Тестирование работы анализатора
print("\nТестирование работы анализатора для русского языка:")
analyzed = es.indices.analyze(
    index="tourists",
    analyzer="russian_custom",  # Используем наш кастомный анализатор
    text="Съешь ещё этих мягких французских булок, да выпей чаю"  # Тестовый текст
)

# Выводим результат токенизации
print("Результат анализа текста:")
for token in analyzed["tokens"]:
    print(token["token"])
