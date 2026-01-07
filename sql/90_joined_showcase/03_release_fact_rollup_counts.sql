-- Release fact rollup with counts (artists / labels) - FIX B (use ON, no USING)
WITH base_releases AS (
  SELECT release_id, title, country, released
  FROM hive.discogs.releases_ref_v6
  WHERE country IS NOT NULL
  ORDER BY release_id
  LIMIT 20000
),
artist_roll AS (
  SELECT
    ra.release_id,
    count(DISTINCT a.artist_id) AS n_artists,
    array_agg(DISTINCT a.name) AS artists
  FROM hive.discogs.release_artists_v1 ra
  JOIN base_releases br
    ON br.release_id = ra.release_id
  JOIN hive.discogs.artist_name_map_v1 am
    ON am.norm_name = ra.artist_norm
  JOIN hive.discogs.artists_v1 a
    ON a.artist_id = am.artist_id
  GROUP BY 1
),
label_roll AS (
  SELECT
    rl.release_id,
    count(DISTINCT rl.label_norm) AS n_labels,
    array_agg(DISTINCT rl.label_name) AS labels
  FROM hive.discogs.release_label_xref_v1 rl
  JOIN base_releases br
    ON br.release_id = rl.release_id
  GROUP BY 1
)
SELECT
  br.release_id,
  br.title,
  br.country,
  br.released,
  ar.n_artists,
  lr.n_labels,
  ar.artists,
  lr.labels
FROM base_releases br
LEFT JOIN artist_roll ar
  ON ar.release_id = br.release_id
LEFT JOIN label_roll lr
  ON lr.release_id = br.release_id;
