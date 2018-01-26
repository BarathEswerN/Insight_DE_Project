from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient
import json

access_token = "YOUR_ACCESS_TOKEN"
access_token_secret =  "YOUR_ACCESS_TOKEN_SECRET"
consumer_key =  "YOUR_CONSUMER_KEY"
consumer_secret =  "YOUR_CONSUMER_KEY_SECRET"

class StdOutListener(StreamListener):
    def on_data(self, data):
    	 
        #tweet = json.loads(data)
        producer.send_messages("Brands", data.encode('utf-8'))
        print(data)
        return True
    def on_error(self, status):
        print (status)


ipfile = open('ip_addresses.txt', 'r')
ips = ipfile.read()[:-1]
ipfile.close()
ips = ips.split(',')

#IP instances are loaded from external file
kafka = KafkaClient('YOUR_IP_ADDRESS:PORT')

producer = SimpleProducer(kafka)

l = StdOutListener()

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

stream = Stream(auth, l)

#Array should be loaded from an external file
search_term_arr = ['Pepsi', 'Coke', 'Nike', 'Apple', 'Samsung']

stream.filter(languages=["en"], track=search_term_arr)

