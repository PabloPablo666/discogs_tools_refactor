-- Label hierarchy rollup
SELECT
  parent_label_name,
  count(*) AS n_children
FROM hive.discogs.labels_ref_v10
WHERE parent_label_id IS NOT NULL
GROUP BY 1
ORDER BY n_children DESC
LIMIT 50;
