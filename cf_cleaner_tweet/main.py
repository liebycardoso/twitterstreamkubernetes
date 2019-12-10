from google.cloud import bigquery


import pandas as pd
from stop_words import get_stop_words
import re
import base64
from textblob import TextBlob, Word
from langdetect import detect
from datetime import datetime, date

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
        text =  ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", text).split())
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

def clean_textblob(sentence):
    stop_words = list(get_stop_words('en'))
    sentence = re.split('\W+', sentence)
    sentence =  " ".join([w for w in sentence if not w in stop_words])
    return "".join([Word(w).lemmatize() for w in sentence])

  
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

        df.drop_duplicates(subset=['user_screen_name', 'text'], keep='first', inplace=True)

        df["hashtags"] = df.text.apply(extract_hashtag)
        df["hashtags_count"]  = df.hashtags.apply(count_hashtag)

        # Use regex to clean the text
        df["text"] = df.text.apply(cleaner_txt)
        df['text'] = df['text'].apply(clean_textblob)
        df.drop(df[df.text ==''].index, inplace=True)

        df["description"] = df.description.apply(cleaner_txt) 

        df['lang'] = df.text.apply(check_language)
        df.drop(df[df.lang !='en'].index, inplace=True)
        del df['lang']

        df[['polarity', 'subjectivity']] = df['text'].apply(lambda text: pd.Series(TextBlob(text).sentiment))
        df['sentiment'] = df.polarity.apply(get_sentiment_description)

        df['description_count'] = df.description.apply(count_word)
        df.fillna(value={'description': 'neutral'}, inplace=True)

        df['description'] = df.description.apply(clean_textblob)
        df[['desc_polarity', 'desc_subjectivity']] = df['description'].apply(lambda text: pd.Series(TextBlob(text).sentiment))
        df['desc_sentiment'] = df.desc_polarity.apply(get_sentiment_description)

        df = df[['text', 'quote_count', 'reply_count', 'retweet_count', 'favorite_count',
            'user_screen_name', 'user_location', 'user_verified',
            'user_followers_count', 'user_friends_count', 'user_listed_count',
            'user_favourites_count', 'user_statuses_count', 'description', 'date',
            'hashtags', 'hashtags_count', 'polarity', 'subjectivity', 'sentiment', 'creation_days', 'description_count', 'desc_polarity',
            'desc_subjectivity', 'desc_sentiment']]
                   
        try:
            #table_id = os.environ['TABLE_ID']
            #project_id = os.environ['PROJECT_ID']
            df.to_gbq(destination_table="t.t", project_id="prj", if_exists='append')
        except Exception as e:
            print(e)
    

if __name__ == "__main__":
    tweet_cleaner('x', 'y')