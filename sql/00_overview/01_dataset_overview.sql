-- Dataset overview: row counts for main Discogs tables
SELECT 'artists_v1_typed' AS table_name, count(*) AS n FROM hive.discogs.artists_v1_typed
UNION ALL SELECT 'masters_v1_typed', count(*) FROM hive.discogs.masters_v1_typed
UNION ALL SELECT 'artist_aliases_v1_typed', count(*) FROM hive.discogs.artist_aliases_v1_typed
UNION ALL SELECT 'artist_memberships_v1_typed', count(*) FROM hive.discogs.artist_memberships_v1_typed
UNION ALL SELECT 'releases_ref_v6', count(*) FROM hive.discogs.releases_ref_v6
UNION ALL SELECT 'labels_ref_v10', count(*) FROM hive.discogs.labels_ref_v10
UNION ALL SELECT 'collection', count(*) FROM hive.discogs.collection;
