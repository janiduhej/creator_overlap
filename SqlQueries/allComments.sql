SELECT comment_id,
       creator_channel_id,
       channel_id
FROM il.yt_comments
WHERE created_at::DATE BETWEEN now()::DATE - INTERVAL '365 day' AND now()::DATE;