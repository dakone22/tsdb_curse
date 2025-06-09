# Курсовая

# Установка

- Только для linux/WSL
- Использовался python 3.10
- Использовать [совместимую](https://community.cloudera.com/t5/Community-Articles/Spark-and-Java-versions-Supportability-Matrix/ta-p/383669) Java версию со спарком (8/11/17, не выше!)

Склонировать.

Создание виртуального окружения для Python (vevn) и установка необходимых пакетов:
```bash
python3.10 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Сначала надо поднять контейнеры:
```bash
docker compose up -d
```

Примечание: в зависимости от версий docker, могут быть разные варианты запуска: старый `docker-compose` как отдельная команда или новый `docker compose` (как модуль docker).

После поднятия ещё какое-то время надо на то, чтобы все сервисы инициализировались.

# Выполнение

Для выполнения просто запуск скриптов по порядку:
```bash
python scripts/0-generate_data.py
python scripts/1-es_create_index.py
# ...
```
 и так далее по списку

Скрипт 4 предполагает блокирующую паузу после выполнения и интерактивное взаимодействие со SparkUI (через веб-интерфейс)

## Дополнительно

`scripts/run_cypher.py` используется для исполнения запросов. Например:
```bash
python scripts/run_cypher.py neo4j/queries/popular_tours.cypher
```

`spark_select_alt.py` - альтернатива скрипту 6: перед запросом данные читаются не из ES, а из HDFS. (Получается иной план с broadcast)

# После работы

Остановка контейнеров:
```bash
docker compose down
```

Остановка контейнеров и удаление всех данных (volume):
```bash
docker compose down -v
```
