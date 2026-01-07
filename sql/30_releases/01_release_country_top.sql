-- Top release countries
SELECT
  country,
  count(*) AS n_releases
FROM hive.discogs.releases_ref_v6
WHERE country IS NOT NULL
  AND trim(country) <> ''
GROUP BY 1
ORDER BY n_releases DESC
LIMIT 50;
