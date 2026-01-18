USE hive.discogs;

WITH checks AS (

  -- -------------------------
  -- artists_v1
  -- -------------------------
  SELECT
    'artists_v1.rows' AS check_name,
    'CRITICAL'        AS severity,
    CAST(count(*) AS VARCHAR) AS value,
    (count(*) > 0)    AS ok,
    NULL              AS details
  FROM artists_v1

  UNION ALL
  SELECT
    'artists_v1.null_artist_id',
    'CRITICAL',
    CAST(count_if(artist_id IS NULL) AS VARCHAR),
    (count_if(artist_id IS NULL) = 0),
    NULL
  FROM artists_v1

  UNION ALL
  SELECT
    'artists_v1.empty_name',
    'WARN',
    CAST(count_if(name IS NULL OR trim(name)='') AS VARCHAR),
    (count_if(name IS NULL OR trim(name)='') = 0),
    NULL
  FROM artists_v1

  -- -------------------------
  -- artist_aliases_v1 (strong FK on artist_id)
  -- -------------------------
  UNION ALL
  SELECT
    'artist_aliases_v1.orphan_artist_id_rows',
    'CRITICAL',
    CAST(count(*) AS VARCHAR),
    (count(*) = 0),
    NULL
  FROM artist_aliases_v1 a
  LEFT JOIN artists_v1 ar ON a.artist_id = ar.artist_id
  WHERE a.artist_id IS NOT NULL AND ar.artist_id IS NULL

  -- weak profiling (not failing)
  UNION ALL
  SELECT
    'artist_aliases_v1.orphan_alias_rows',
    'INFO',
    CAST(count(*) AS VARCHAR),
    TRUE,
    'alias_id may be orphan in Discogs'
  FROM artist_aliases_v1 a
  LEFT JOIN artists_v1 ar ON a.alias_id = ar.artist_id
  WHERE a.alias_id IS NOT NULL AND ar.artist_id IS NULL

  -- -------------------------
  -- artist_memberships_v1 (both ids should exist as artists)
  -- -------------------------
  UNION ALL
  SELECT
    'artist_memberships_v1.orphan_member_ids',
    'WARN',
    CAST(count(*) AS VARCHAR),
    (count(*) = 0),
    NULL
  FROM artist_memberships_v1 m
  LEFT JOIN artists_v1 a ON m.member_id = a.artist_id
  WHERE a.artist_id IS NULL

  UNION ALL
  SELECT
    'artist_memberships_v1.orphan_group_ids',
    'WARN',
    CAST(count(*) AS VARCHAR),
    (count(*) = 0),
    NULL
  FROM artist_memberships_v1 m
  LEFT JOIN artists_v1 a ON m.group_id = a.artist_id
  WHERE a.artist_id IS NULL

  -- -------------------------
  -- labels_ref_v10
  -- -------------------------
  UNION ALL
  SELECT
    'labels_ref_v10.null_label_id',
    'CRITICAL',
    CAST(count_if(label_id IS NULL) AS VARCHAR),
    (count_if(label_id IS NULL) = 0),
    NULL
  FROM labels_ref_v10

  UNION ALL
  SELECT
    'labels_ref_v10.duplicate_label_id_groups',
    'INFO',
    CAST(count(*) AS VARCHAR),
    TRUE,
    'duplicates may exist if not canonicalised'
  FROM (
    SELECT label_id
    FROM labels_ref_v10
    GROUP BY 1
    HAVING count(*) > 1
  ) t

  -- -------------------------
  -- masters_v1
  -- -------------------------
  UNION ALL
  SELECT
    'masters_v1.null_master_id',
    'CRITICAL',
    CAST(count_if(master_id IS NULL) AS VARCHAR),
    (count_if(master_id IS NULL) = 0),
    NULL
  FROM masters_v1

  UNION ALL
  SELECT
    'masters_v1.duplicate_master_ids',
    'CRITICAL',
    CAST(count(*) AS VARCHAR),
    (count(*) = 0),
    NULL
  FROM (
    SELECT master_id
    FROM masters_v1
    GROUP BY 1
    HAVING count(*) > 1
  ) t

  -- -------------------------
  -- releases_ref_v6
  -- -------------------------
  UNION ALL
  SELECT
    'releases_ref_v6.null_release_id',
    'CRITICAL',
    CAST(count_if(release_id IS NULL) AS VARCHAR),
    (count_if(release_id IS NULL) = 0),
    NULL
  FROM releases_ref_v6

  UNION ALL
  SELECT
    'releases_ref_v6.duplicate_release_ids',
    'CRITICAL',
    CAST(count(*) AS VARCHAR),
    (count(*) = 0),
    NULL
  FROM (
    SELECT release_id
    FROM releases_ref_v6
    GROUP BY 1
    HAVING count(*) > 1
  ) t

  -- -------------------------
  -- Cross: masters main_release_id exists in releases (warn)
  -- -------------------------
  UNION ALL
  SELECT
    'masters_v1.orphan_main_release_id',
    'WARN',
    CAST(count(*) AS VARCHAR),
    TRUE,
    'expected small >0 sometimes'
  FROM masters_v1 m
  LEFT JOIN releases_ref_v6 r ON m.main_release_id = r.release_id
  WHERE m.main_release_id IS NOT NULL AND r.release_id IS NULL

  -- -------------------------
  -- Warehouse sanity (active)
  -- -------------------------
  UNION ALL
  SELECT
    'warehouse.artist_name_map_v1.orphan_artist_ids',
    'WARN',
    CAST(count(*) AS VARCHAR),
    (count(*) = 0),
    NULL
  FROM artist_name_map_v1 nm
  LEFT JOIN artists_v1 a ON nm.artist_id = a.artist_id
  WHERE a.artist_id IS NULL

  UNION ALL
  SELECT
    'warehouse.release_artists_v1.orphan_release_ids',
    'WARN',
    CAST(count(*) AS VARCHAR),
    (count(*) = 0),
    NULL
  FROM release_artists_v1 ra
  LEFT JOIN releases_ref_v6 r ON ra.release_id = r.release_id
  WHERE r.release_id IS NULL

  UNION ALL
  SELECT
    'warehouse.release_style_xref_v1.orphan_release_ids',
    'WARN',
    CAST(count(*) AS VARCHAR),
    (count(*) = 0),
    NULL
  FROM release_style_xref_v1 x
  LEFT JOIN releases_ref_v6 r ON x.release_id = r.release_id
  WHERE r.release_id IS NULL

  UNION ALL
  SELECT
    'warehouse.release_genre_xref_v1.orphan_release_ids',
    'WARN',
    CAST(count(*) AS VARCHAR),
    (count(*) = 0),
    NULL
  FROM release_genre_xref_v1 x
  LEFT JOIN releases_ref_v6 r ON x.release_id = r.release_id
  WHERE r.release_id IS NULL
)

SELECT *
FROM checks
ORDER BY
  CASE severity
    WHEN 'CRITICAL' THEN 1
    WHEN 'WARN' THEN 2
    WHEN 'INFO' THEN 3
    ELSE 4
  END,
  check_name;
