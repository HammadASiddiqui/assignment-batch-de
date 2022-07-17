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
def insert_into_table(data_array, table_key):
    print("______________INGESTION start_______________________")
    
    ingest = table_key.insert()
    
    try:
        ingest.execute(data_array)
    except Exception as e:
        print('[ERROR] INGESTION ERROR', str(e))
    
    print("_____________________________________")



#process tweets from pagenated data
def process_page(page_results, tweets, users):
    batch_tweets=[]
    batch_users=[]
    for i, tweet in enumerate(page_results):
        tweet_dict = {'tweet_id': str(tweet._json['id_str']),
        'created_at': tweet._json['created_at'],
        'tweet_text': str(tweet._json['text']).replace("'",'').replace("%(D)",''),
        'tweet_language': str(tweet._json['lang']),
        'favorite_count': tweet._json['favorite_count'],
        'retweet_count': tweet._json['retweet_count'],
        'raw_json':str(tweet._json),
        'user_id': str(tweet._json['user']['id_str'])}
        batch_tweets.append(tweet_dict)
        
        user_dict= {
        'user_id': str(tweet._json['user']['id_str']),
        'user_name': str(tweet._json['user']['name']).replace("'",''),
        'user_url': str(tweet._json['user']['url']),}

        batch_users.append(user_dict)


    insert_into_table( batch_tweets, tweets)
    insert_into_table( batch_users, users)



if __name__ == '__main__':
    print('application started')

    with open('db_creds.yaml') as f:
        db_creds_dict = yaml.load(f, Loader=SafeLoader)
    
    db = init_db(db_creds_dict)

    
    with open('api_auth.yaml') as f:
        keys = yaml.load(f, Loader=SafeLoader)
    
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

    users = Table('users', metadata,
        Column('user_id', String, primary_key=True),
        Column('user_name', String),
        Column('user_url', String),
    )
    
    
    OAUTH_KEYS = {'consumer_key': keys['consumer_key'], 'consumer_secret': keys['consumer_secret'],
                  'access_token_key': keys['access_token_key'], 'access_token_secret': keys['access_token_secret']}
    auth = tweepy.OAuthHandler(
        OAUTH_KEYS['consumer_key'], OAUTH_KEYS['consumer_secret'])

    api = tweepy.API(auth)

    keyword = "ikea"

    for i, page in enumerate(tweepy.Cursor(api.search_tweets, q=keyword, count=20, lang='en').pages(40)):
        process_page(page, tweets, users, db)
        print('-------------', i, '--------------')
