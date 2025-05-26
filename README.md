# Курсовая

# Установка

- Только для linux/WSL
- Используйте python 3.10

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
