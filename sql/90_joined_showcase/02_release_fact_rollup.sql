-- Release fact rollup with aggregated artist and label dimensions
WITH base_releases AS (
  SELECT release_id, title, country, released
  FROM hive.discogs.releases_ref_v6
  WHERE country IS NOT NULL
  ORDER BY release_id
  LIMIT 20000
),
artist_dim AS (
  SELECT
    ra.release_id,
    array_agg(DISTINCT a.name) AS artists
  FROM hive.discogs.release_artists_v1 ra
  JOIN base_releases br ON br.release_id = ra.release_id
  JOIN hive.discogs.artist_name_map_v1 am ON am.norm_name = ra.artist_norm
  JOIN hive.discogs.artists_v1 a ON a.artist_id = am.artist_id
  GROUP BY 1
),
label_dim AS (
  SELECT
    rl.release_id,
    array_agg(DISTINCT rl.label_name) AS labels
  FROM hive.discogs.release_label_xref_v1 rl
  JOIN base_releases br ON br.release_id = rl.release_id
  GROUP BY 1
)
SELECT
  br.*,
  ad.artists,
  ld.labels
FROM base_releases br
LEFT JOIN artist_dim ad USING (release_id)
LEFT JOIN label_dim  ld USING (release_id);
