from google.cloud import bigquery


import pandas as pd
from stop_words import get_stop_words
import re
import base64
from textblob import TextBlob
from langdetect import detect
from datetime import timedelta, datetime, date

def return_date(x):
    return datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').date()



def extract_hashtag(text):
    if (text):
        hashtag = re.findall(r"#(\w+)", text)
    return hashtag

def check_language(text):
    return detect(text)

def count_hashtag(text):
    return len(text)

def cleaner_txt(text):
        
    ''' 
    Function to clean:
     - Http links
     - @ mention
     - special caracter
     - RT
    '''
    if isinstance(text, str):   
        text = text.lower()
        text =  ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(^rt)", " ", text).split())
    else:
        text = ''
    return text
        
    return text
def tokenization(text):
    return re.split('\W+', text)

def count_word(text):
    
    if isinstance(text, str):
        text = re.split('\W+', text)
        count = len(text)
    else:
        count = 0
    return count

  
def get_sentiment_description(polarity): 
    ''' 
    Utility function to classify sentiment of passed tweet 
    using textblob's sentiment method 
    '''
    # set sentiment 
    if polarity > 0: 
        return 'positive'
    elif polarity == 0: 
        return 'neutral'
    else: 
        return 'negative'

def tweet_cleaner(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    #pubsub_message = base64.b64decode(event['data']).decode('utf-8')
  
    client = bigquery.Client()
    # Perform a query.
    
    sql = (
        ' SELECT  '+
        'text, quote_count, reply_count, '+
        'retweet_count, favorite_count, user_screen_name, user_location, '+
        'user_verified, user_followers_count, user_friends_count, '+
        'user_listed_count, user_favourites_count, user_statuses_count, '+
        'description, '+
        'CAST(created_at as DATE) as date, ' +
        'DATE_DIFF(CURRENT_DATE(), DATE(created_at), DAY) as daysdf '+
        'FROM capstonettw.tweet ' +
        'WHERE DATE(created_at) <= CURRENT_DATE() ' 
    #    'LIMIT 1'
    #    ' WHERE DATE(created_at) = "2019-11-14"'
    )

    # Insert the query result into a dataframe
    df = pd.read_gbq(sql, dialect='standard')

    if len(df) > 0:

        #df["date"] = df.date.apply(return_date)
        
        # Extract all the words that starts with #
        df["hashtags"] = df.text.apply(extract_hashtag)
        df["hashtags_count"]  = df.hashtags.apply(count_hashtag)

        # Use regex to clean the text
        df["text"] = df.text.apply(cleaner_txt) 

        df["description"] = df.description.apply(cleaner_txt) 

        

        df.drop(df[df.text ==''].index, inplace=True)
        df['lang'] = df.text.apply(check_language)

        

        df.drop(df[df.lang !='en'].index, inplace=True)

        del df['lang']
        df.drop_duplicates(subset=['user_screen_name', 'text'], keep='first', inplace=False)

        try:
            def remove_stop_word(word_list):
                filter_word =  [w for w in word_list if not w in stop_words]

                return  ' '.join(filter_word)

            stop_words = list(get_stop_words('en'))         
        except Exception as e:
            print(e)

        # Split the sentence into a array of words
        df['text'] = df['text'].apply(tokenization)
        # Remove Stop words
        df['text'] = df.text.apply(remove_stop_word)

        df['polarity'] = 0.0
        df['subjectivity'] = 0.0

        df[['polarity', 'subjectivity']] = df['text'].apply(lambda text: pd.Series(TextBlob(text).sentiment))

        df['sentiment'] = df.polarity.apply(get_sentiment_description)

        #df["creation_days"] = df.date.apply(lambda x: (date.today() - x).days)
        df["creation_days"] = df.daysdf

        del df["daysdf"]
        df['description_count'] = df.description.apply(count_word)

        df['desc_polarity'] = 0.0
        df['desc_subjectivity'] = 0.0

        df[['desc_polarity', 'desc_subjectivity']] = df['description'].apply(lambda text: pd.Series(TextBlob(text).sentiment))

        df['desc_sentiment'] = df.desc_polarity.apply(get_sentiment_description)

        df.fillna(value={'description': 'neutral'}, inplace=True)
        df['desc_polarity'] = 0.0
        df['desc_subjectivity'] = 0.0

        df[['desc_polarity', 'desc_subjectivity']] = df['description'].apply(lambda text: pd.Series(TextBlob(text).sentiment))

        df['desc_sentiment'] = df.desc_polarity.apply(get_sentiment_description)
                    
        try:
            #table_id = os.environ['TABLE_ID']
            #project_id = os.environ['PROJECT_ID']
            df.to_gbq(destination_table="t.t", project_id="prj", if_exists='append')
        except Exception as e:
            print(e)
    

if __name__ == "__main__":
    tweet_cleaner('x', 'y')