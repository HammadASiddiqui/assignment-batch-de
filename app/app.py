import yaml
from yaml.loader import SafeLoader
import tweepy
from sqlalchemy import *
from datetime import datetime, timedelta


# intialize connection parameters for postgres database
def init_db(db_creds):
    #extract credentials from dictionary for connecting DB
    db=None
    db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_creds['db_user'], db_creds['db_pass'], db_creds['db_host'], db_creds['db_port'], db_creds['db_name'])
    try:
        db = create_engine(db_string)
    except:
        print("[DB CONNECT ERR]")
    finally:
        return db

#insert data for the exception in bulkload(row by row)
def insert_into_table(data_array, table_key):
    for record in data_array:
        #Sqlalchemy ORM based insert function
        ingest = table_key.insert()
        #execution of insertion statement
        try:
            ingest.execute(record)
        except Exception as e:
            print('[ERROR] INGESTION ERROR', str(e))


#insert tweet's data
def bulkload_insertion_into_table(data_array, table_key):
    print("______________ batch ingestion start _______________________")
    
    #Sqlalchemy ORM based insert function
    ingest = table_key.insert()
    
    #execution of bulkload insertion statement on whole data array of objects
    try:
        ingest.execute(data_array)
    except Exception as e:
        print('[ERROR] INGESTION ERROR', str(e))
        insert_into_table(data_array, table_key)
    
    print("______________ batch ingestion ends _______________________")



#process tweets from pagenated data
def process_page(page_results, tweets, users):
    

    batch_tweets=[]
    batch_users=[]
    for i, tweet in enumerate(page_results):
        
        #creating dictionary contaning info from the extracted tweets JSON 
        tweet_dict = {'tweet_id': str(tweet._json['id_str']),
        'created_at': tweet._json['created_at'],
        'tweet_text': str(tweet._json['text']),
        'tweet_language': str(tweet._json['lang']),
        'favorite_count': tweet._json['favorite_count'],
        'retweet_count': tweet._json['retweet_count'],
        'raw_json':str(tweet._json),
        'user_id': str(tweet._json['user']['id_str'])}
        
        #appending the tweets object data array
        batch_tweets.append(tweet_dict)
        
        
        #creating dictionary contaning info from the extracted tweets JSON for users
        user_dict= {
        'user_id': str(tweet._json['user']['id_str']),
        'user_name': str(tweet._json['user']['name']),
        'user_url': str(tweet._json['user']['url']),}

        #appending the user object data array
        batch_users.append(user_dict)
        


    #bulkload ingestion for the whole batch of 20 tweets of a page
    bulkload_insertion_into_table( batch_users, users)
    bulkload_insertion_into_table( batch_tweets, tweets)
    



if __name__ == '__main__':
    print('application started')

    #reading DB credential file for extracting credentials
    try:
        with open('db_creds.yaml') as f:
            db_creds_dict = yaml.load(f, Loader=SafeLoader)
    except:
        print("db_creds.yaml file not found")
    
    #create connection to DB
    db = init_db(db_creds_dict)

    
    #reading api auth file for extracting api keys
    try:
        with open('api_auth.yaml') as f:
            keys = yaml.load(f, Loader=SafeLoader)
    except:
        print("api_auth.yaml not found")
    
    #sqlalchemy function to relate metadata info from db with program based ORM structures
    metadata = MetaData(db)

    #structural defination for tweets table fro ORM based ingestion
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

    #structural defination for user table fro ORM based ingestion
    users = Table('users', metadata,
        Column('user_id', String, primary_key=True),
        Column('user_name', String),
        Column('user_url', String),
    )
    
    #creating auth keys from the read dictionary
    OAUTH_KEYS = {'consumer_key': keys['consumer_key'], 'consumer_secret': keys['consumer_secret'],
                  'access_token_key': keys['access_token_key'], 'access_token_secret': keys['access_token_secret']}
    auth = tweepy.OAuthHandler(
        OAUTH_KEYS['consumer_key'], OAUTH_KEYS['consumer_secret'])

    #setting auth for API calling
    api = tweepy.API(auth)

    keyword = "ikea"

    #iterate over the API results and pagenate them in size of 20 tweets per page extracting the week's tweet
    for i, page in enumerate(tweepy.Cursor(api.search_tweets, q=keyword,until=datetime.strftime(datetime.now(), '%Y-%m-%d'), lang='en').pages()):
        process_page(page, tweets, users)
        print('-------------', i, '--------------')
