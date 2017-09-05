import tweepy
import random
import unicodedata
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

#Authentication details
consumer_key = 'hkMI3BNVtLiRRaoQjnD7Tv8ER'
consumer_secret = 'IvsKRgdey1QkaWFiRppGQ87cBr9DvE5xtj27MHCL6iVCVOwCwi'
access_token = '2915283505-oW8hxeGIkOomYTlh1wnoRzYRWJnRykaI2EFg6d8'
access_secret = 'xGBbpRuJDeXQr9hT1YZgrVVajhx1vV4lBMoj2QrznL3yY'

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth)

KAFKA_HOST = 'localhost:9092'
TOPIC = 'tweet'

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

#default locations if it is not specified in the tweet info
def getloc( index ):
	loc =["32.776, -96.797", "36.778, -119.417","40.712, -74.005","43.076, -107.290","40.417, -82.907","39.321, -111.093","47.606, -122.332","46.879, -110.362","43.784, -88.787","33.448, -112.074"]
	return loc[index]


class StdOutListener(StreamListener):
    def on_status(self, status): 
	   if(status.coordinates!= None):
			loc=str(status.coordinates)
	   else:
		    loc=getloc(random.randint(0,9))
	   if "obama" in status.text:
			producer.send(TOPIC,  unicodedata.normalize('NFKD', status.text+"::obama::"+str(status.coordinates)+"::"+status.timestamp_ms+"::"+loc).encode('ascii','ignore')  )
			return True
	   else:
			producer.send(TOPIC,  unicodedata.normalize('NFKD', status.text+"::trump::"+str(status.coordinates)+"::"+status.timestamp_ms+"::"+loc).encode('ascii','ignore')  )
			return True
	   
l = StdOutListener()	   
stream = tweepy.Stream(auth, l)
#limit the tweets to just USA and language as english
stream.filter(track=['obama','trump']).filter(locations=[-125.0011, 24.9493, -66.9326, 49.5904]).filter(language=en)