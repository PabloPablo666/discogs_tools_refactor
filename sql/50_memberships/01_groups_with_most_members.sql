-- Groups with most distinct members
SELECT
  group_id,
  max(group_name) AS group_name,
  count(DISTINCT member_id) AS n_members
FROM hive.discogs.artist_memberships_v1
GROUP BY 1
ORDER BY n_members DESC
LIMIT 50;
