import subprocess

# Базовая команда
runner = ['curl', '-H', 'Content-Type: application/json']

# Список команд с указанием URL и файла запроса
commands = [
    ['-X', 'POST', 'http://localhost:9200/tours/_search', '-d', '@es/queries/tours_aggregation.json'],
    ['-X', 'POST', 'http://localhost:9200/tourists/_search', '-d', '@es/queries/tourists_aggregation.json'],
]

# Запуск каждой команды
for cmd_args in commands:
    cmd = runner + cmd_args
    print(f"Запуск: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    print("\n"+"="*10)