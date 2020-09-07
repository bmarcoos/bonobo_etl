import tweepy
from pprint import pprint
import bonobo
import json
import time
from datetime import datetime
import sys
import sqlalchemy as db

sys.path.append("../conf")
from db_conn import engine

# connection to Data Base
connection = engine.connect()
metadata = db.MetaData(bind=engine)
twitter_con = db.Table('twitter', metadata, autoload=True)

# twitter api credentials
consumer_key = 'XXXX'
consumer_secret = 'XXXX'
access_token = 'XXXX'
access_token_secret = 'XXX'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

user = '@DFinanciero'
since = 920469485441683458
max_ = 1265638790137249792
api = tweepy.API(auth)


def limit_handled(cursor):
    while True:
        try:
            yield cursor.next()
        except tweepy.RateLimitError:
            time.sleep(15 * 60)


def extract():
    '''
    Extract tweets from Twitter API from Diario Financiero twitter account 

    '''

    statuses_list = []
    try:
        for statuses in limit_handled(tweepy.Cursor(api.user_timeline, screen_name=user, since_id=since, max_id=max_, tweet_mode='extended').items(100)):
            statuses_list.append(statuses._json)
    except:
        print('Extraction completed')

    yield from statuses_list


def process(tweet):
    '''
    Get tweet data.

    Return:
        tweet_id: (integer) tweet ID
        created_at: (string)
            Ex: 'Sat May 30 03:30:10 +0000 2020'
        full_text: (string)
        url: (string or None)
        hashtags: (list or None)
    '''

    tweet_id = tweet['id']
    created_at = datetime.strptime(tweet['created_at'],'%a %b %d %H:%M:%S +0000 %Y')
    full_text = tweet['full_text']
    
    url = None
    if tweet['entities'].get('media') != None:
        url = tweet['entities']['media'][0]['url']
        
    hashtags = None
    if len(tweet['entities']['hashtags']) > 0:
        hashtags = tweet['entities']['hashtags']

    yield tweet_id, created_at, full_text, url, hashtags


def load(tweet_id, created_at, full_text, url, hashtags):
    '''
    Load tweet to database
    '''

    try: # fails in case of duplicates
        connection.execute(twitter_con.insert(), {"id": tweet_id,
                                        "created_at": created_at,
                                        "full_text": full_text,
                                        "url": url,
                                        "hashtags": hashtags
                                            })
    except:
        pass


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    graph.add_chain(extract, process, load)

    return graph


def get_services(**options):
    """
    This function builds the services dictionary, which is a simple dict of names-to-implementation used by bonobo
    for runtime injection.

    It will be used on top of the defaults provided by bonobo (fs, http, ...). You can override those defaults, or just
    let the framework define them. You can also define your own services and naming is up to you.

    :return: dict
    """
    return {}


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
