import collections
import datetime
import time
import json

from apiclient import discovery
import dateutil.parser
import httplib2

#from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials

SCOPES = ['https://www.googleapis.com/auth/bigquery',
          'https://www.googleapis.com/auth/pubsub']
NUM_RETRIES = 3



def get_credentials():
    """Get the Google credentials needed to access our services."""
    credentials = GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
            credentials = credentials.create_scoped(SCOPES)
    return credentials


def create_bigquery_client(credentials):
    """Build the bigquery client."""
    http = httplib2.Http()
    credentials.authorize(http)
    return discovery.build('bigquery', 'v2', http=http)


def create_pubsub_client(credentials):
    """Build the pubsub client."""
    http = httplib2.Http()
    credentials.authorize(http)
    return discovery.build('pubsub', 'v1beta2', http=http)

def filter_tweet(data, return_type):

    filter_data = {
                    "created_at"	: data["created_at"],
                    "id"	: data.get("id"),
                    "text"	: data.get("text"),
                    "quote_count"	: data.get("quote_count"),
                    "reply_count"	: data.get("reply_count"),
                    "retweet_count"	: data.get("retweet_count"),
                    "favorite_count"	    : data.get("favorite_count"),
                    "hashtags"	            : data.get("entities", {}).get("hashtags"),
                    "user_screen_name"	    : data.get("user", {}).get("screen_name"),
                    "user_location"	        : data.get("user", {}).get("location"),
                    "user_verified"	        : data.get("user", {}).get("verified"),
                    "user_followers_count"	: data.get("user", {}).get("followers_count"),
                    "user_friends_count"	: data.get("user", {}).get("friends_count"),
                    "user_listed_count"	    : data.get("user", {}).get("listed_count"),
                    "user_favourites_count"	: data.get("user", {}).get("favourites_count"),
                    "user_statuses_count"	: data.get("user", {}).get("statuses_count"),
                    "rt_quote_count"	: data.get("retweeted_status", {}).get("quote_count"),
                    "rt_reply_count"	: data.get("retweeted_status", {}).get("reply_count"),
                    "rt_retweet_count"	: data.get("retweeted_status", {}).get("retweet_count"),
                    "rt_favorite_count"	: data.get("retweeted_status", {}).get("favorite_count"),
                    "rt_text"	        : data.get("retweeted_status", {}).get("extended_tweet", {}).get("full_text")
                }

    if return_type == "str":
        # Convert dict to string
        return json.dumps(filter_data)
    else:
        return filter_data



def flatten(lst):
    """Helper function used to massage the raw tweet data."""
    #basestring=[]
    for el in lst:
        if (isinstance(el, collections.Iterable) and
                not isinstance(el, (str, bytes))):
            for sub in flatten(el):
                yield sub
        else:
            yield el


def cleanup(data):
    """Do some data massaging."""
    if isinstance(data, dict):
        newdict = {}
        for k, v in data.items():
            if (k == 'hashtags') and isinstance(v, list):
                # flatten list
                newdict[k] = list(flatten(v))
            elif k == 'created_at' and v:
                newdict[k] = str(dateutil.parser.parse(v))
            # temporarily, ignore some fields not supported by the
            # current BQ schema.
            # TODO: update BigQuery schema
            elif (k == ''):
                pass
            elif v is False:
                newdict[k] = v
            else:
                if k and v:
                    newdict[k] = cleanup(v)
        return newdict
    elif isinstance(data, list):
        newlist = []
        for item in data:
            newdata = cleanup(item)
            if newdata:
                newlist.append(newdata)
        return newlist
    else:
        return data


def bq_data_insert(bigquery, project_id, dataset, table, tweets):
    """Insert a list of tweets into the given BigQuery table."""
    try:
        rowlist = []
        # Generate the data that will be sent to BigQuery
        for item in tweets:
            item_row = {"json": item}
            rowlist.append(item_row)
        body = {"rows": rowlist}
        # Try the insertion.
        response = bigquery.tabledata().insertAll(
                projectId=project_id, datasetId=dataset,
                tableId=table, body=body).execute(num_retries=NUM_RETRIES)
        # print "streaming response: %s %s" % (datetime.datetime.now(), response)
        return response
        # TODO: 'invalid field' errors can be detected here.
    except Exception as e1 :
        print("Giving up: %s" % e1)
