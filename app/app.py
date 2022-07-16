import yaml
from yaml.loader import SafeLoader
import tweepy
from sqlalchemy import *

# intialize connection parameters for postgres database
def init_db(db_creds):
    db=None
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_creds['db_user'], db_creds['db_pass'], db_creds['db_host'], db_creds['db_port'], db_creds['db_name'])
    try:
        db = create_engine(db_string)
    except:
        print("[DB CONNECT ERR]")
    finally:
        return db

#insert user's data
def insert_into_users_table(tweet_json, db):
    user_dict= {
        'user_id': str(tweet_json['user']['id_str']),
        'user_name': str(tweet_json['user']['name']).replace("'",''),
        'user_url': str(tweet_json['user']['url']),
    }
    query = '''INSERT INTO users (user_id, user_name, user_url) 
    VALUES (
        '{user_id}',
        '{user_name}',
        '{user_url}'
    );'''.format(**user_dict)
    try:
        db.execute(text(query))
    except:
        pass

#insert tweet's data
def insert_into_tweets_table(tweet_json, db):
    tweet_dict = {'tweet_id': str(tweet_json['id_str']),
    'created_at': tweet_json['created_at'],
    'tweet_text': str(tweet_json['text']).replace("'",'').replace("%(D)",''),
    'tweet_language': str(tweet_json['lang']),
    'favorite_count': tweet_json['favorite_count'],
    'retweet_count': tweet_json['retweet_count'],
    'raw_json':str(tweet_json),
    'user_id': str(tweet_json['user']['id_str'])}
    print("_____________________________________")
    print("Ingesting data for ",tweet_dict['tweet_id']," >>>>>")
    metadata = MetaData(db)

    tweets = Table('tweets', metadata,
        Column('tweet_id', String, primary_key=True),
        Column('created_at', TIMESTAMP, primary_key=True),
        Column('tweet_text', String),
        Column('tweet_language', String),
        Column('favorite_count', Integer),
        Column('retweet_count', Integer),
        Column('raw_json', String),
        Column('user_id', String),
    )
    ingest = tweets.insert()
    
    ingest.execute(tweet_id=tweet_dict['tweet_id'],
    created_at=tweet_dict['created_at'],
    tweet_text=tweet_dict['tweet_text'],
    tweet_language=tweet_dict['tweet_language'],
    favorite_count=tweet_dict['favorite_count'],
    retweet_count=tweet_dict['retweet_count'],
    raw_json=tweet_dict['raw_json'],
    user_id=tweet_dict['user_id'])
    print("_____________________________________")



#process tweets from pagenated data
def process_page(page_results, db):
    for i, tweet in enumerate(page_results):
        insert_into_tweets_table( tweet._json, db)
        insert_into_users_table( tweet._json, db)



if __name__ == '__main__':
    print('application started')

    with open('db_creds.yaml') as f:
        db_creds_dict = yaml.load(f, Loader=SafeLoader)
    
    db = init_db(db_creds_dict)

    
    with open('api_auth.yaml') as f:
        keys = yaml.load(f, Loader=SafeLoader)
    
    

    OAUTH_KEYS = {'consumer_key': keys['consumer_key'], 'consumer_secret': keys['consumer_secret'],
                  'access_token_key': keys['access_token_key'], 'access_token_secret': keys['access_token_secret']}
    auth = tweepy.OAuthHandler(
        OAUTH_KEYS['consumer_key'], OAUTH_KEYS['consumer_secret'])

    api = tweepy.API(auth)

    keyword = "ikea"
    tweets = []

    for i, page in enumerate(tweepy.Cursor(api.search_tweets, q=keyword, count=20, lang='en').pages(50)):
        process_page(page, db)
        print('-------------', i, '--------------')
