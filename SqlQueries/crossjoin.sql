WITH a AS (
    SELECT DISTINCT channel_id
    FROM il.yt_channels
)
  SELECT DISTINCT
    CASE WHEN a.channel_id > b.channel_id
      THEN a.channel_id
    ELSE b.channel_id END AS a,
    CASE WHEN a.channel_id > b.channel_id
      THEN b.channel_id
    ELSE a.channel_id END AS b
  FROM a
    CROSS JOIN a AS b
  WHERE a.channel_id != b.channel_id;