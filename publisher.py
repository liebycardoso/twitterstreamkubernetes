
import base64
import datetime
import os
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
from tweepy.streaming import StreamListener
import json


import utils

# Reference:
# https://github.com/GoogleCloudPlatform/kubernetes-bigquery-python/blob/master/pubsub/pubsub-pipe-image/twitter-to-pubsub.py

# Get twitter credentials from file.

consumer_key = os.environ['CONSUMERKEY']
consumer_secret = os.environ['CONSUMERSECRET']
access_token = os.environ['ACCESSTOKEN']
access_token_secret = os.environ['ACCESSTOKENSEC']

PUBSUB_TOPIC = os.environ['PUBSUB_TOPIC']

NUM_RETRIES = 3

def publish(client, pubsub_topic, data_lines):
    """
    Add a message to a Google PubSub topic.
    
    JSON format:
    {
        "messages": [
            {
                object (PubsubMessage)
            }
        ]
    }
    """

    messages = []
    for line in data_lines:
        pub = base64.urlsafe_b64encode(line.encode()).decode()
        messages.append({'data': pub})
    body = {'messages': messages}
    resp = client.projects().topics().publish(
            topic=pubsub_topic, body=body).execute(num_retries=NUM_RETRIES)
    return resp


class StdOutListener(StreamListener):
    """
    Gets data received from the stream tweepy function.
    Filter data and publish into a PubSub topic

    Tweepy doc:
    In Tweepy, an instance of tweepy.Stream establishes 
    a streaming session and routes messages to StreamListener instance. 
    """

    count = 0
    twstring = ''
    tweets = []
    batch_size = 50
    total_tweets = 10000
    client = utils.create_pubsub_client(utils.get_credentials())

    def write_to_pubsub(self, tw):        
        publish(self.client, PUBSUB_TOPIC, tw)

    def on_data(self, data):
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

        # filter only meaningful features               
        data =  utils.filter_tweet(json.loads(data), "str")

        self.tweets.append(data)

        if len(self.tweets) >= self.batch_size:
            print(len(self.tweets))
            #self.write_to_pubsub(self.tweets)
            self.tweets = []
        
        self.count += 1
        if self.count > self.total_tweets:
            return False
        if (self.count % 1000) == 0:
            print('count is: %s at %s' % (self.count, datetime.datetime.now()))
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':

    listener = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    #
    # print('stream mode is: %s' % os.environ['TWSTREAMMODE'])

    stream = Stream(auth, listener)
    # set up the streaming depending upon whether our mode is 'sample', which
    # will sample the twitter public stream. If not 'sample', instead track
    # the given set of keywords.
    # This environment var is set in the 'twitter-stream.yaml' file.
    if "x" == 'sample':
        stream.sample()
    else:
        stream.filter(
            track=['#cdnpoli', '#elxn43','#CanadaElection2019', 
            '#canpoli', '#CanadianElection', '#JustinTrudeau',
            '#jagmeetsingh', '#AndrewScheer', 'CPC_HQ', 'liberal_party',
            '#ChooseForward', 'ndp', 'InItForYou']
        )
