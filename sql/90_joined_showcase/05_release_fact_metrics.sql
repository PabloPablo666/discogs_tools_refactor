-- Release fact metrics (materialisable) - safe for single-node Trino
WITH base_releases AS (
  SELECT release_id
  FROM hive.discogs.releases_ref_v6
  WHERE country IS NOT NULL
  ORDER BY release_id
  LIMIT 20000
),
artist_metrics AS (
  SELECT
    ra.release_id,
    count(DISTINCT am.artist_id) AS n_artists
  FROM hive.discogs.release_artists_v1 ra
  JOIN base_releases br
    ON br.release_id = ra.release_id
  JOIN hive.discogs.artist_name_map_v1 am
    ON am.norm_name = ra.artist_norm
  GROUP BY 1
),
label_metrics AS (
  SELECT
    rl.release_id,
    count(DISTINCT rl.label_norm) AS n_labels
  FROM hive.discogs.release_label_xref_v1 rl
  JOIN base_releases br
    ON br.release_id = rl.release_id
  GROUP BY 1
)
SELECT
  r.release_id,
  r.title,
  r.country,
  r.released,
  coalesce(am.n_artists, 0) AS n_artists,
  coalesce(lm.n_labels, 0) AS n_labels
FROM hive.discogs.releases_ref_v6 r
JOIN base_releases br
  ON br.release_id = r.release_id
LEFT JOIN artist_metrics am
  ON am.release_id = r.release_id
LEFT JOIN label_metrics lm
  ON lm.release_id = r.release_id;
