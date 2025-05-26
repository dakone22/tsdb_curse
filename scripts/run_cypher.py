import os
import sys
from pathlib import Path

from neo4j import GraphDatabase

NEO4J_HOST      = os.environ.get("NEO4J_HOST", "localhost")
NEO4J_BOLT_PORT = os.environ.get("NEO4J_BOLT_PORT", "7687")
NEO4J_USER      = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD  = os.environ.get("NEO4J_PASSWORD", "password")

NEO4J_BOLT_URI = f"bolt://{NEO4J_HOST}:{NEO4J_BOLT_PORT}"

def run_cypher_script(filepath):
    if not Path(filepath).is_file():
        print(f"Ошибка: файл '{filepath}' не существует.")
        sys.exit(1)

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            script = f.read()

        driver = GraphDatabase.driver(
            NEO4J_BOLT_URI,
            auth=(NEO4J_USER, NEO4J_PASSWORD),
        )

        statements = [
            stmt.strip()
            for stmt in script.split(";")
            if stmt.strip()
        ]

        with driver.session() as session:
            for stmt in statements:
                print(f"Выполняем:\n{stmt[:60]}...")
                result = session.run(stmt)
                for record in result:
                    print(record)

        print("Cypher-скрипт выполнен успешно.")
    except Exception as e:
        print(f"Ошибка при выполнении Cypher-скрипта: \n{e}")
    finally:
        driver.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Использование: python {sys.argv[0]} <путь_к_файлу.cypher>")
        sys.exit(1)

    script_path = sys.argv[1]
    run_cypher_script(script_path)