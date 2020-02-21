import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time

access_token = "YOUR KEY"
access_tokensecret = "YOUR KEY"
consumer_key= "YOUR KEY"
consumer_secret = "YOUR KEY"

class analytics(StreamListener):
	def on_data(self,data):
		try:
			saveFile = open('wwe.json','a')
			saveFile.write(data)
			saveFile.write(', \n')
			saveFile.close()
			return True

		except BaseException as except1:
			print ('data parsing error,',str(except1))
			time.sleep(5)

	def on_error(self,status):
		print (status)

verification = OAuthHandler(consumer_key,consumer_secret)
verification.set_access_token(access_token,access_tokensecret)
stream_twitter = Stream(verification,analytics())
stream_twitter.filter(track=['WWE','WWE RAW','WWE Universe','RAW Story','John Cena','Roman Reigns','Rock','Triple H','Smackdown'])
