import os

from elasticsearch import Elasticsearch, exceptions

ES_HOST = os.environ.get("ES_HOST", "localhost")
ES_PORT = os.environ.get("ES_PORT", "9200")

es = Elasticsearch(f"http://{ES_HOST}:{ES_PORT}")

# Общие настройки анализатора
analysis_settings = {
    "analysis": {
        "filter": {
            "russian_stop": {
                "type": "stop",
                "stopwords": "_russian_"
            },
            "russian_snowball": {
                "type": "snowball",
                "language": "Russian"
            }
        },
        "analyzer": {
            "russian_custom": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": [
                    "lowercase",
                    "russian_stop",
                    "russian_snowball"
                ]
            }
        }
    }
}

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
        es.indices.create(index=name, settings=body['settings'], mappings=body['mappings'])
        print(f"Индекс '{name}' успешно создан.")
    except exceptions.ElasticsearchException as e:
        print(f"Ошибка при создании индекса '{name}':", e)

if __name__ == "__main__":
    create_index("tourists", tourists_body)
    create_index("tours", tours_body)
