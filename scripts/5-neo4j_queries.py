import subprocess

# Основной скрипт для выполнения Cypher-запросов через run_cypher.py
runner = ['python', 'scripts/run_cypher.py']

# Список Cypher-скриптов для выполнения:
# 1. Импорт данных в Neo4j
# 2. Запрос для поиска популярных туров
cypher_files = [
    'neo4j/queries/import_to_neo4j.cypher',
    'neo4j/queries/popular_tours.cypher',
]

# Выполняем каждый Cypher-скрипт по очереди
for file in cypher_files:
    # Формируем команду для выполнения скрипта
    cmd = runner + [file]
    print(f"Запуск: {' '.join(cmd)}")
    
    # Выполняем команду через subprocess
    # check=True - вызывает исключение при ошибке выполнения
    subprocess.run(cmd, check=True)

# Сообщение о завершении с ссылкой на веб-интерфейс Neo4j
print("Вы можете посмотреть результаты в веб-интерфейсе Neo4j: http://localhost:7474/browser/")
