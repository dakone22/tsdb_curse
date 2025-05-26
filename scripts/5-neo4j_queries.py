import subprocess

runner = ['python', 'scripts/run_cypher.py']
cypher_files = [
    'neo4j/queries/import_to_neo4j.cypher',
    'neo4j/queries/popular_tours.cypher',
]

for file in cypher_files:
    cmd = runner + [file]
    print(f"Запуск: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

print("You can visit http://localhost:7474/browser/ or something...")