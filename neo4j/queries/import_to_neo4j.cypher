CALL {
  LOAD CSV WITH HEADERS FROM 'file:///tours.csv' AS row
  MERGE (tr:Tour {id: row.id_тура})
    SET tr.name    = row.название,
        tr.country = row.страна,
        tr.price   = toFloat(row.стоимость)
} IN TRANSACTIONS OF 500 ROWS;

CALL {
  LOAD CSV WITH HEADERS FROM 'file:///tourists.csv' AS row
  MERGE (t:Tourist {id: row.id_туриста})
    SET t.name = row.персональные_данные_туриста
} IN TRANSACTIONS OF 500 ROWS;

CALL {
  LOAD CSV WITH HEADERS FROM 'file:///purchases.csv' AS row
  MATCH (t:Tourist {id: row.id_туриста})
  MATCH (tr:Tour    {id: row.id_тура})
  MERGE (t)-[:BOUGHT {date: row.дата_тура}]->(tr)
} IN TRANSACTIONS OF 500 ROWS;
