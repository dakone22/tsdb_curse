"""
Скрипт для выполнения агрегаций в Elasticsearch.
Запускает предопределенные запросы из файлов:
- es/queries/tours_aggregation.json - агрегации по турам
- es/queries/tourists_aggregation.json - агрегации по туристам
Использует curl для отправки запросов к REST API Elasticsearch.
"""

import subprocess

# Базовая команда curl с заголовком для JSON
runner = ['curl', '-H', 'Content-Type: application/json']

# Список команд для выполнения:
# Каждая команда содержит:
# - Метод HTTP (POST)
# - URL индекса Elasticsearch
# - Файл с телом запроса (@ означает чтение из файла)
commands = [
    ['-X', 'POST', 'http://localhost:9200/tours/_search', '-d', '@es/queries/tours_aggregation.json'],
    ['-X', 'POST', 'http://localhost:9200/tourists/_search', '-d', '@es/queries/tourists_aggregation.json'],
]

# Выполняем все команды по очереди
for cmd_args in commands:
    # Формируем полную команду из базовой и аргументов
    cmd = runner + cmd_args
    
    # Выводим выполняемую команду для лога
    print(f"Запуск: {' '.join(cmd)}")
    
    # Выполняем команду через subprocess
    # check=True - вызывает исключение при ошибке выполнения
    subprocess.run(cmd, check=True)
    
    # Разделитель между результатами запросов
    print("\n"+"="*10)
