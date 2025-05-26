MATCH (t:Tourist)-[b:BOUGHT]->(tr:Tour)
RETURN t, b, tr;