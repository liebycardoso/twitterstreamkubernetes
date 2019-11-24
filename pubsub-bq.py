#!/usr/bin/env python
"""This script grabs tweets from a PubSub topic, and stores them in BiqQuery
using the BigQuery Streaming API.
"""

import base64
import json
import os
import time
from google.cloud import bigquery
from google.cloud import pubsub

# Get the project ID and pubsub topic from the environment variables set in
# the 'bigquery-controller.yaml' manifest.

class tweet_subscriber():
    PROJECT_ID = os.environ['PROJECT_ID']
    BQ_DATASET= os.environ['BQ_DATASET'] 
    BQ_TABLE = os.environ['BQ_TABLE']
    tweets = []
    CHUNK = 50
    NUM_RETRIES = 3

    def write_tweets_to_bq(self, tweets):
        client = bigquery.Client()
        dataset_ref = client.dataset(self.BQ_DATASET, project=self.PROJECT_ID)
        table_ref = dataset_ref.table(self.BQ_TABLE)
        table = client.get_table(table_ref)  # API call
        schema = [
                bigquery.SchemaField('created_at', 'TIMESTAMP', mode='NULLABLE'),
                bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('text', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('quote_count', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('reply_count', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('retweet_count', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('favorite_count', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('user_screen_name', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('user_location', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('user_verified', 'BOOLEAN', mode='NULLABLE'),
                bigquery.SchemaField('user_followers_count', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('user_friends_count', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('user_listed_count', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('user_favourites_count', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('user_statuses_count', 'INTEGER', mode='NULLABLE'),
                bigquery.SchemaField('description', 'STRING', mode='NULLABLE')
            ]
        
        errors = client.insert_rows(table, tweets, selected_fields = schema)
            
        if not errors:
            print('Loaded {} row(s) into BIGQYERY'.format(len(tweets)))
        else:
            print('Errors:')
            for error in errors:
                print(error)


    # decodes the message from PubSub
    def pull_tweets(self, data):
        
        stream = base64.urlsafe_b64decode(data)
        twraw = json.loads(stream)
        twmessages = twraw.get('messages')
        for message in twmessages:
            self.tweets.append(message['data'])
        
        if len(self.tweets) >= self.CHUNK:
            self.write_tweets_to_bq(self.tweets)
            self.tweets = []
                                    

    def receive_tweets(self, subscription_name):
        subscriber = pubsub.SubscriberClient()
        subscription_path = subscriber.subscription_path(self.PROJECT_ID, subscription_name)# pylint: disable=maybe-no-member
        
        def callback(message):
            print('Received message: {}'.format(message))
            self.pull_tweets(message)
            message.ack()

        future = subscriber.subscribe(subscription_path, callback=callback)
        print('Listening for messages on {}'.format(subscription_path))

        try:
            future.result()
        except Exception as e:
            print(
                'Listening for messages on {} threw an Exception: {}'.format(
                    subscription_name, e))
            raise

        while True:
            time.sleep(60)


if __name__ == '__main__':
    
    PUBSUB_TOPIC = os.environ['PUBSUB_TOPIC']
    topic_info = PUBSUB_TOPIC.split('/')
    topic_name = topic_info[-1]
    sub_name = "tweets-%s" % topic_name
    rt = tweet_subscriber()
    rt.receive_tweets(sub_name)