
import base64
from datetime import datetime
import os
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
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

NUM_RETRIES = 3

def publish(client, topic_path, data_lines):
    """Publish to the given pubsub topic."""
    messages = []
    for line in data_lines:
        messages.append({'data': line})
    body = {'messages': messages}
    str_body = json.dumps(body)
    data = base64.urlsafe_b64encode(bytearray(str_body, 'utf8'))
    client.publish(topic_path, data=data)


class StdOutListener(StreamListener):
    """A listener handles tweets that are received from the stream.
    This listener dumps the tweets into a PubSub topic
    """
    batch_settings = pubsub_v1.types.BatchSettings(
    max_bytes=1024,  # One kilobyte
    max_latency=1,   # One second
)

    publisher = pubsub_v1.PublisherClient(batch_settings)
    topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)# pylint: disable=maybe-no-member

    count = 0
    twstring = ''
    tweets = []
    batch_size = 50
    total_tweets = 1000000
    
    def write_to_pubsub(self, tw):
        publish(self.publisher, PUBSUB_TOPIC, tw)

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
        try:
            data = json.loads(data)
            # filter only meaningful features 
            
            filter_data = (                
                        str(datetime.strptime(data["created_at"], '%a %b %d %H:%M:%S %z %Y')),
                        int(data.get("id",0)),
                        data.get("text"),
                        int(data.get("quote_count", 1)),
                        int(data.get("reply_count", 1)),
                        int(data.get("retweet_count", 1)),
                        int(data.get("favorite_count",1)),
                        "empty",
                        data.get("user", {}).get("screen_name", "empty"),
                        data.get("user", {}).get("location","empty"),
                        bool(data.get("user", {}).get("verified",False)),
                        int(data.get("user", {}).get("followers_count",1)),
                        int(data.get("user", {}).get("friends_count",1)),
                        int(data.get("user", {}).get("listed_count",1)),
                        int(data.get("user", {}).get("favourites_count",1)),
                        int(data.get("user", {}).get("statuses_count",1)),
                        int(data.get("retweeted_status", {}).get("quote_count",1)),
                        int(data.get("retweeted_status", {}).get("reply_count",1)),
                        int(data.get("retweeted_status", {}).get("retweet_count",1)),
                        int(data.get("retweeted_status", {}).get("favorite_count",1)),
                        data.get("retweeted_status", {}).get("extended_tweet", {}).get("full_text", "empty")
            )          
                    
            self.tweets.append(filter_data)
        except Exception as e:
            print(e)
            pass
        
        
        if len(self.tweets) >= self.batch_size:
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
            print('count is: %s at %s' % (self.count, datetime.datetime.now()))
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    listener = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, listener)
    # set up the streaming depending upon whether our mode is 'sample', which
    # will sample the twitter public stream. If not 'sample', instead track
    # the given set of keywords.
    # This environment var is set in the 'twitter-stream.yaml' file.
    if os.environ['TWSTREAMMODE'] == 'sample':
        stream.sample()
    else:
        stream.filter(
            track=['#cdnpoli', '#elxn43','#CanadaElection2019', 
            '#canpoli', '#CanadianElection', '#JustinTrudeau',
            '#jagmeetsingh', '#AndrewScheer', 'CPC_HQ', 'liberal_party',
            '#ChooseForward', 'ndp', 'InItForYou']
        )
