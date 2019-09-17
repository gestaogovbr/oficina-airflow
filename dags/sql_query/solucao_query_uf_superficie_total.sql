DROP TABLE IF EXISTS uf_superficie_total;

SELECT uf, SUM(superficie_ha) as total_superficie
INTO uf_superficie_total
FROM indigenas
GROUP BY uf
ORDER BY 2 DESC;
