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

# После работы

Остановка контейнеров:
```bash
docker compose down
```

Остановка контейнеров и удаление всех данных (volume):
```bash
docker compose down -v
```
