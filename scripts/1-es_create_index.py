"""
Скрипт для создания индексов в Elasticsearch для хранения данных о турах и туристах.
Создает два индекса: 'tourists' и 'tours' с русскоязычным анализом текста.
"""

import os
from elasticsearch import Elasticsearch, exceptions

# Настройки подключения к Elasticsearch
ES_HOST = os.environ.get("ES_HOST", "localhost")  # Хост Elasticsearch
ES_PORT = os.environ.get("ES_PORT", "9200")      # Порт Elasticsearch

# Инициализация клиента Elasticsearch
es = Elasticsearch(f"http://{ES_HOST}:{ES_PORT}")

# Настройки анализатора для русского языка
analysis_settings = {
    "analysis": {
        "filter": {
            "russian_stop": {
                "type": "stop",                  # Фильтр для стоп-слов
                "stopwords": "_russian_"         # Встроенный список русских стоп-слов
            },
            "russian_snowball": {
                "type": "snowball",              # Стеммер для русского языка
                "language": "Russian"            # Язык стемминга
            }
        },
        "analyzer": {
            "russian_custom": {                  # Кастомный анализатор
                "type": "custom",                # Тип - кастомный анализатор
                "tokenizer": "standard",         # Стандартный токенизатор
                "filter": [                      # Цепочка фильтров
                    "lowercase",                 # Приведение к нижнему регистру
                    "russian_stop",              # Удаление стоп-слов
                    "russian_snowball"           # Стемминг
                ]
            }
        }
    }
}

# Структура индекса 'tourists' (данные о туристах)
tourists_body = {
    "settings": analysis_settings,  # Применяем настройки анализатора
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

# Структура индекса 'tours' (данные о турах)
tours_body = {
    "settings": analysis_settings,  # Применяем настройки анализатора
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
    """
    Создает индекс в Elasticsearch с указанными настройками.
    Если индекс уже существует - удаляет его перед созданием.
    
    Args:
        name (str): Название индекса
        body (dict): Настройки и маппинг индекса
    """
    try:
        # Проверяем существование индекса
        if es.indices.exists(index=name):
            print(f"Индекс '{name}' уже существует, удаляем...")
            es.indices.delete(index=name)
        
        print(f"Создаём индекс '{name}'...")
        # Создаем индекс с указанными настройками
        es.indices.create(index=name, settings=body['settings'], mappings=body['mappings'])
        print(f"Индекс '{name}' успешно создан.")
    except exceptions.ElasticsearchException as e:
        print(f"Ошибка при создании индекса '{name}':", e)

if __name__ == "__main__":
    # Создаем оба индекса при запуске скрипта
    create_index("tourists", tourists_body)
    create_index("tours", tours_body)
