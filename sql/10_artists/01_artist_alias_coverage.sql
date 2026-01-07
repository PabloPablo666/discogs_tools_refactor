-- Alias coverage per artist (distribution + counts)
WITH alias_counts AS (
  SELECT
    artist_id,
    count(*) AS n_aliases
  FROM hive.discogs.artist_aliases_v1
  GROUP BY 1
)
SELECT
  approx_percentile(n_aliases, 0.50) AS p50_aliases,
  approx_percentile(n_aliases, 0.90) AS p90_aliases,
  approx_percentile(n_aliases, 0.99) AS p99_aliases,
  count(*) AS n_artists_with_aliases
FROM alias_counts;
