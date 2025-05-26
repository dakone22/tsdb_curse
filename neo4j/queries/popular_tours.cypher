MATCH (t:Tourist)-[b:BOUGHT]->(tr:Tour)
RETURN tr.name AS tour, count(b) AS sales
ORDER BY sales DESC
LIMIT 5;
