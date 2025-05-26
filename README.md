# Курсовая

# Установка

- Только для linux/WSL
- Использовался python 3.10
- Использовать [совместимую](https://community.cloudera.com/t5/Community-Articles/Spark-and-Java-versions-Supportability-Matrix/ta-p/383669) Java версию со спарком (8/11/17, не выше!)

```bash
python3.10 -m venv .venv
source .vevn/bin/activate
pip install -r requirements.txt
```

Сначала надо поднять контейнеры:
```bash
docker-compose up -d
```

Остановка контейнеров:
```bash
docker-compose down
```


Остановка контейнеров и удаление всех данных:
```bash
docker-compose down -v
```

# Выполнение

```bash
python scripts/0-generate_data.py
```
 и так далее по списку

4 предлагает интерактивное взаимодействие со SparkUI (через веб-интерфейс)
