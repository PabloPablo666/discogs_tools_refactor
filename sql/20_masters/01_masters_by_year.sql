-- Masters distribution by year
SELECT
  year,
  count(*) AS n_masters
FROM hive.discogs.masters_v1
WHERE year IS NOT NULL
  AND year BETWEEN 1900 AND 2030
GROUP BY 1
ORDER BY year;
