CREATE TABLE IF NOT EXISTS tweets (
    tweet_id TEXT,
    created_at TIMESTAMP,
    tweet_text TEXT,
    tweet_language TEXT,
    favorite_count BIGINT,
    retweet_count BIGINT,
    raw_json TEXT,
    user_id TEXT
)PARTITION BY RANGE (created_at);

alter table tweets add primary key (tweet_id, created_at);

CREATE TABLE tweets_y2022m07 PARTITION OF tweets
    FOR VALUES FROM ('2022-07-01') TO ('2022-08-01');

CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    user_name TEXT,
    user_url TEXT
);
