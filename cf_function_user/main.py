import pandas as pd
import re
import base64
from google.cloud import bigquery

def aggregate_user(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    #pubsub_message = base64.b64decode(event['data']).decode('utf-8')
  
    
    client = bigquery.Client()
    # Perform a query.
    sql = (
        ' SELECT user_screen_name, '+
        ' user_favourites_count, user_followers_count, user_listed_count, '+
        ' user_statuses_count, user_verified, creation_days, ' +
        ' description_count, desc_polarity as polarity, desc_subjectivity as subjectivity, desc_sentiment as sentiment '+
        ' FROM capstonettw.sentiment ' 
    #    'WHERE DATE(user_created_at) <= CURRENT_DATE() ' +
    #    ' WHERE date(date) = "2019-11-14" ' +
    #    'LIMIT 5'
    )

    # Insert the query result into a dataframe
    df = pd.read_gbq(sql, dialect='standard')

    print(len(df))

    df.drop_duplicates(subset=['user_screen_name'], keep='last', inplace=True)
    df = pd.concat([df,pd.get_dummies(df['sentiment'], prefix='sentiment')],axis=1)

    df.drop(['sentiment'], axis=1, inplace=True)

    columns_lst = df.columns

    dummies = ['positive',  'neutral', 'negative']

    for item in dummies:
        if item not in columns_lst:
            name = 'sentiment_' + item
            df[name] = 0

    df['user_verified'] = df.user_verified.astype(int)

    df = df[['user_screen_name', 'user_followers_count', 'user_listed_count',
       'user_favourites_count', 'user_statuses_count', 'description_count',
       'user_verified', 'creation_days', 'polarity', 'subjectivity',
       'sentiment_positive', 'sentiment_neutral', 'sentiment_negative']]
    
    print(len(df))
    print(df.columns)


    try:
        #table_id = os.environ['TABLE_ID']
        #project_id = os.environ['PROJECT_ID']
        df.to_gbq(destination_table="capstonettw.user", project_id="heroic-gamma-254018", if_exists='append')
    except Exception as e:
        print(e)

if __name__ == "__main__":
    aggregate_user('x', 'y')