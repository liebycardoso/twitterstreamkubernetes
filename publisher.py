
import base64
from datetime import datetime
import os
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
from google.cloud import pubsub_v1

# Reference:
# https://github.com/GoogleCloudPlatform/kubernetes-bigquery-python/blob/master/pubsub/pubsub-pipe-image/twitter-to-pubsub.py

# Get twitter credentials from file.


consumer_key = os.environ['CONSUMERKEY']
consumer_secret = os.environ['CONSUMERSECRET']
access_token = os.environ['ACCESSTOKEN']
access_token_secret = os.environ['ACCESSTOKENSEC']
PUBSUB_TOPIC = os.environ['PUBSUB_TOPIC']
PROJECT_ID = os.environ['PROJECT_ID']
TWSTREAMMODE = os.environ['TWSTREAMMODE']

NUM_RETRIES = 3
NUMBER_OF_TWEETS = 4000
BATCH_SIZE = 50

def publish(client, topic_path, data_lines):
    """Publish to the given pubsub topic."""
    messages = []
    for line in data_lines:
        messages.append({'data': line})
    body = {'messages': messages}
    str_body = json.dumps(body)
    data = base64.urlsafe_b64encode(bytearray(str_body, 'utf8'))
    client.publish(topic_path, data=data)

def filter_data(data, type):
    data = json.loads(data)
    try:  
        
        # filter only meaningful features 
        if 'retweeted_status' in data:
            pass
        else:                             
            data = (                
                        str(datetime.strptime(data["created_at"], '%a %b %d %H:%M:%S %z %Y')),
                        int(data.get("id",0)),
                        data.get(type),
                        int(data.get("quote_count", 0)),
                        int(data.get("reply_count", 0)),
                        int(data.get("retweet_count", 0)),
                        int(data.get("favorite_count",0)),
                        data.get("user", {}).get("screen_name", "null"),
                        data.get("user", {}).get("location","null"),
                        bool(data.get("user", {}).get("verified",False)),
                        int(data.get("user", {}).get("followers_count",0)),
                        int(data.get("user", {}).get("friends_count",0)),
                        int(data.get("user", {}).get("listed_count",0)),
                        int(data.get("user", {}).get("favourites_count",0)),
                        int(data.get("user", {}).get("statuses_count",0)),                            
                        int(data.get("user", {}).get("description",0))
                    )          
    except Exception as e:
        print(e)
        pass
    return data

def process_timeline(username, n_weets):
    tml= []
    count = 0

    for item in tweepy.Cursor(api.user_timeline,
                             screen_name=username,
                             tweet_mode="extended",
                             languages=["en"]).items(n_weets):
        try:
            data = filter_data(json.dumps(item._json),"full_text")
            if len(data) <= 15:
                tml.append(data)
            
            if len(tml) >= BATCH_SIZE:
                publish(publisher, PUBSUB_TOPIC, tml)
                tml = []

            count += 1
        except Exception as e:
            print(e)

class StdOutListener(StreamListener):
    """A listener handles tweets that are received from the stream.
    This listener dumps the tweets into a PubSub topic
    """
    
    count = 0
    twstring = ''
    tweets = []
    total_tweets = 10000
    
    def write_to_pubsub(self, tw):
        publish(publisher, PUBSUB_TOPIC, tw)

    def on_data(self, data):
        """What to do when tweet data is received."""
        """
        Override Tweepy on_data method to manipulate 
        the data content before publish to the 
        pub-sub topic

        Tweepy doc:
        The on_data method of a stream listener receives all messages and calls 
        functions according to the message type. The default StreamListener 
        can classify most common twitter messages and routes them to appropriately 
        named methods, but these methods are only stubs.
        """
        
        data = filter_data(data, "text")
        if len(data) <= 15:
            self.tweets.append(data)

                
        if len(self.tweets) >= BATCH_SIZE:
            self.write_to_pubsub(self.tweets)
            self.tweets = []

        self.count += 1
        # if we've grabbed more than total_tweets tweets, exit the script.
        # If this script is being run in the context of a kubernetes
        # replicationController, the pod will be restarted fresh when
        # that happens.
        if self.count >= self.total_tweets:
            return False
        if (self.count % 100) == 0:
            print('count is: %s' % (self.count))
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':

    batch_settings = pubsub_v1.types.BatchSettings(
    max_bytes=1024,  # One kilobyte
    max_latency=1,   # One second
    )

    publisher = pubsub_v1.PublisherClient(batch_settings)
    topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)# pylint: disable=maybe-no-member

    listener = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, listener)
    api = tweepy.API(auth)
    #follow=['14260960', '253340075', '256360738'])
    #['JustinTrudeau', 'AndrewScheer', 'theJagmeetSingh']
    
    if TWSTREAMMODE == "timeline":
        process_timeline("JustinTrudeau", NUMBER_OF_TWEETS)
        process_timeline("AndrewScheer", NUMBER_OF_TWEETS)
        process_timeline("theJagmeetSingh", NUMBER_OF_TWEETS)

    stream.filter(languages=["en"],
            track=['#cdnpoli', '#elxn43','#CanadaElection2019', 
            '#canpoli', '#CanadianElection', '#JustinTrudeau',
            '#jagmeetsingh', '#AndrewScheer', 'CPC_HQ', 'liberal_party',
            '#ChooseForward', 'ndp', 'InItForYou']
        )
    
    