with channels as (select DISTINCT channel_id
                  from il.yt_comments),

     skeleton as (SELECT c.channel_id as a, b.channel_id as b
                  FROM channels c
                           CROSS JOIN channels b
                  WHERE c.channel_id != b.channel_id)

SELECT a,b
FROM skeleton s;