import pandas as pd
import re
import base64
from google.cloud import bigquery

def run_model_losgistic(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    #pubsub_message = base64.b64decode(event['data']).decode('utf-8')
  
    
    client = bigquery.Client()
    # Perform a query.

    sql = (
     ' SELECT user_screen_name, predicted_label, favourites_count, followers_count, '
    + ' listed_count, statuses_count, '
    + ' verified, description_count, polarity, subjectivity  '
    + ' FROM ml.PREDICT(MODEL capstonettw.spot_bot_no_days, ('
    + '   SELECT '
    + '    user_screen_name, user_favourites_count as favourites_count , '
    + '    user_followers_count as followers_count, '
    + '    user_listed_count as listed_count, '
    + '    user_statuses_count as statuses_count, '
    + '    user_verified as verified, description_count, polarity, '
    + '    subjectivity, sentiment_negative, sentiment_neutral, sentiment_positive'
    + '   FROM '
    + '     capstonettw.user'
    + ' ))'
    )

    try:
        df = pd.read_gbq(sql, dialect='standard')
        df.to_gbq(destination_table="capstonettw.predicted", project_id="heroic-gamma-254018", if_exists='append')
    except Exception as e:
        print(e)

if __name__ == '__main__':
    run_model_losgistic('x','y')