-- Top styles (explode CSV-like styles column)
WITH exploded AS (
  SELECT
    trim(s) AS style
  FROM hive.discogs.releases_ref_v6
  CROSS JOIN UNNEST(split(coalesce(styles, ''), ',')) AS t(s)
)
SELECT
  style,
  count(*) AS n_mentions
FROM exploded
WHERE style <> ''
GROUP BY 1
ORDER BY n_mentions DESC
LIMIT 50;
