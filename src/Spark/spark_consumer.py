import os
import sys
import json
import MySQLdb as mdb
from twitter import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext  
from pyspark.streaming.kafka import KafkaUtils
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from unidecode import unidecode
from dateutil.parser import parse


#Load it from file 

brokers = "YOUR BROKERS IP:PORT"
topics = "Brands"
analyzer = SentimentIntensityAnalyzer()
con = mdb.connect('YOUR_IP_ADDRESS', 'YOUR_USER', 'YOUR_PASSWORD', 'YOUR_DB_NAME');
cursor = con.cursor()


def remove_non_ascii(text):
    return unidecode(unicode(text, encoding = "utf-8"))

# Needs to be populated from file
brands = ['Pepsi', 'Coke', 'Nike', 'Apple', 'Samsung']

def check_brand(text):
    for brand in brands:
        if (brand in text):
            return brand

    return ' '


def extract_data(json_body):
    json_body = json.loads(json_body)
    try:
        tweet_id = json_body['id_str']
        tweet_timestamp = json_body['created_at']
        tweet_text = json_body['text'].encode('utf-8')
        
        tweet_brand = check_brand(tweet_text)          
        if (tweet_brand == ' ') and ('quoted_status' in json_body):
            tweet_brand = check_brand(json_body['quoted_status']['text'])
                  
        tweet_user_location = 'None'
        if ('location' in json_body['user']):
            tweet_user_location = json_body['user']['location']
    except:
        return None

    data = {'id': tweet_id,
            'brand': tweet_brand,
            'timestamp': tweet_timestamp,
            'text': tweet_text,
            'user_location': tweet_user_location
           }

    return data


def insert_into_db(sent_tweets):
    for x in sent_tweets:
        #print(x)
        if(x is None or x['brand'] == ' '):
            sent_tweets.remove(x)
            continue
       
        text = remove_non_ascii(x['text'])
        text = text.replace("'", "''")
        date = parse(x['timestamp']).date()
        #print(date)
        with con:   
            query = "INSERT INTO TWEETDATA VALUES ('%s', '%s', '%s', '%s', '%s')" %(x['id'], x['brand'], text, x['result'], date)
            cursor.execute(query)


def analyzeSentiment(tweet):
     if(tweet is None or tweet['brand'] == ' '):
        return

     score_dict = analyzer.polarity_scores(tweet['text'])
     compoundScr = score_dict.pop('compound', None)
     res = 'neu'
     if (compoundScr < -0.25):
        res = 'neg'
     elif (compoundScr > 0.25):
        res = 'pos'

     tweet['compoundScr'] = compoundScr
     tweet['result'] = res;
     return tweet
    

def process(rdd):
    
    if rdd.count() == 0:
        return

    lines = rdd.map(lambda x : x[1])
    lines = lines.map(lambda x : extract_data(x))       
    lines = lines.map(lambda x : analyzeSentiment(x));

    sent_classified_tweets = lines.collect();
    insert_into_db(sent_classified_tweets)
    
    #code for clustering
    #clustered_tweets = sent_classified_tweets.map(lambda x : cluster_tweets())

    #insert_into_table(clustered_tweets)

sc = SparkContext(appName="Brand")
ssc = StreamingContext(sc,1)

#get stream data from kafka
kafkaStream = KafkaUtils.createDirectStream(ssc, [topics], {"metadata.broker.list": brokers})


kafkaStream.foreachRDD(process) 
ssc.start()
ssc.awaitTermination()
