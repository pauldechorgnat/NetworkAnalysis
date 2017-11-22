
# coding: utf-8

# In[1]:


#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import tweepy
# importing csv writers
import csv
# importing pandas
import pandas as pd
import sys


# In[2]:


consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''


# In[3]:


auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)


# In[7]:


path = 'C:/Users/Marie/Desktop/MSc DSBA/3. Big Data Algorithms and Platforms/'

class CustomStreamListener(tweepy.StreamListener):
    
    def __init__(self, count = None, verbose = 1, 
                 path_tweet = path + 'scraping.csv', 
                 path_author = path +'scraping_author.csv',
                 parent = None):
        super(CustomStreamListener, self).__init__(parent)
        self.count = count
        self.verbose = verbose
        self.path_tweet = path_tweet
        self.path_author = path_author
        
    def on_status(self, status):
        
        if self.verbose ==1:
            print(status.author.screen_name,'|', status.created_at,'|', status.text)
        else :
            if self.verbose ==2:
                print(self.count)
                
        # getting author id
        author_id = status.author.id
        
        #Writing status data
        with open(self.path_tweet, 'a', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([status.id, status.author.id, status.created_at, status.text])
            # tweet (idTweet bigint,text text,date date,iduser bigint)
        
        with open(self.path_author, 'a', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([status.author.id, 
                             status.author.followers_count, 
                             status.author.friends_count, 
                             status.author.created_at])
            # user(iduser bigint, numFollowers int, numFriends int, createdAt date)
        
        
        if self.count != None:
            self.count -=1
            
            if self.count == 0:
                return False
            
    def on_error(self, status_code):
        print('Encountered error with status code:', status_code)
        return True # Don't kill the stream


# In[8]:


with open(path + 'trump_tweets2.txt', 'w', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['tweet_id', 'user_id', 'tweet_date', 'text'])
with open(path + 'trump_tweets_author2.txt', 'w', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['user_id', 'num_followers', 'num_friends', 'created_date'])


# In[9]:


listener = CustomStreamListener(count = 1000, verbose = 0, 
                                path_tweet=path + 'trump_tweets.txt',
                                path_author = path + 'trump_tweets_author.txt')

streamingAPI = tweepy.streaming.Stream(auth, listener)
streamingAPI.filter(track=['trump'])

