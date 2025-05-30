import os
import sys
from pathlib import Path

from neo4j import GraphDatabase

# Настройки подключения к Neo4j через переменные окружения
NEO4J_HOST      = os.environ.get("NEO4J_HOST", "localhost")       # Хост Neo4j
NEO4J_BOLT_PORT = os.environ.get("NEO4J_BOLT_PORT", "7687")      # Порт для подключения по протоколу Bolt
NEO4J_USER      = os.environ.get("NEO4J_USER", "neo4j")          # Имя пользователя
NEO4J_PASSWORD  = os.environ.get("NEO4J_PASSWORD", "password")   # Пароль

# Формирование URI для подключения к Neo4j
NEO4J_BOLT_URI = f"bolt://{NEO4J_HOST}:{NEO4J_BOLT_PORT}"

def run_cypher_script(filepath):
    """
    Выполняет Cypher-скрипт из файла в Neo4j.
    
    Args:
        filepath (str): Путь к файлу с Cypher-скриптом
        
    Процесс:
        1. Проверяет существование файла
        2. Читает содержимое файла
        3. Подключается к Neo4j
        4. Разбивает скрипт на отдельные выражения
        5. Выполняет каждое выражение по очереди
        6. Выводит результаты выполнения
    """
    if not Path(filepath).is_file():
        print(f"Ошибка: файл '{filepath}' не существует.")
        sys.exit(1)

    try:
        # Чтение Cypher-скрипта из файла
        with open(filepath, "r", encoding="utf-8") as f:
            script = f.read()

        # Создание драйвера для подключения к Neo4j
        driver = GraphDatabase.driver(
            NEO4J_BOLT_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD),
        )

        # Разделение скрипта на отдельные выражения по точке с запятой
        statements = [
            stmt.strip()
            for stmt in script.split(";")
            if stmt.strip()
        ]

        # Выполнение каждого выражения в отдельной сессии
        with driver.session() as session:
            for stmt in statements:
                print(f"Выполняем:\n{stmt[:60]}...")  # Выводим первые 60 символов запроса
                result = session.run(stmt)
                for record in result:
                    print(record)  # Вывод результатов выполнения запроса

        print("Cypher-скрипт выполнен успешно.")
    except Exception as e:
        print(f"Ошибка при выполнении Cypher-скрипта: \n{e}")
    finally:
        # Всегда закрываем соединение с Neo4j
        driver.close()

if __name__ == "__main__":
    """
    Точка входа скрипта.
    Ожидает путь к Cypher-скрипту в качестве аргумента командной строки.
    """
    if len(sys.argv) < 2:
        print(f"Использование: python {sys.argv[0]} <путь_к_файлу.cypher>")
        sys.exit(1)

    script_path = sys.argv[1]  # Получаем путь к скрипту из аргументов командной строки
    run_cypher_script(script_path)  # Запускаем выполнение скрипта
