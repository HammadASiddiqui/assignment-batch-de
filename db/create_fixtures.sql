
CREATE TABLE IF NOT EXISTS tweets (
    tweet_id TEXT,
    created_at TIMESTAMP,
    tweet_text TEXT,
    tweet_language TEXT,
    favorite_count BIGINT,
    retweet_count BIGINT,
    user_id TEXT
);

CREATE TABLE IF NOT EXISTS users (
    user_id TEXT,
    user_name TEXT,
    user_url TEXT
);
