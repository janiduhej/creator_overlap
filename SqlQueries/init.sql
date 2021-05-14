DROP TABLE IF EXISTS il.yt_overlap;

CREATE TABLE IF NOT EXISTS il.yt_overlap
(
  channel_a             TEXT NOT NULL,
  channel_b             TEXT NOT NULL,
  absolute_overlap      INTEGER,
  percentage_overlap    NUMERIC,
  CONSTRAINT il_yt_channel_overlap_pk
  PRIMARY KEY (channel_a, channel_b)
);