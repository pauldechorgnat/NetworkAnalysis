
# coding: utf-8

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


# Defining the access tokens -> available on your twitter/dev account
consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

# creating authentification access
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)


# Defining path to save data
path = 'C:/Users/Marie/Desktop/MSc DSBA/3. Big Data Algorithms and Platforms/'

# Defining a custom strean listener that will save tweets and author data within a csv file
class CustomStreamListener(tweepy.StreamListener):
    
    def __init__(self, 
                 count = None, 
                 verbose = 1, 
                 path_tweet = path + 'scraping.csv', 
                 path_author = path +'scraping_author.csv',
                 parent = None):
        """
        count : number of tweets that the streamer will get before stopping
        verbose : if null, does not display any information; 
                  if 1, displays the text of the collected tweets; 
                  if 2 or more displays the countdown
        path_tweet : path to the file to store the tweets
        path_author : path to the file to store the authors
        """
        # Calling constructor of superclass
        super(CustomStreamListener, self).__init__(parent)
        # Setting attributes
        self.count = count
        self.verbose = verbose
        self.path_tweet = path_tweet
        self.path_author = path_author
        
    def on_status(self, status):
        """
        this function is activated when a tweet is got by the streamer
        """
        # information display
        if self.verbose ==1:
            print(status.author.screen_name,'|', status.created_at,'|', status.text)
        else :
            if self.verbose ==2:
                print(self.count)
               
        
        # writing tweet data
        with open(self.path_tweet, 'a', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([status.id, status.author.id, status.created_at, status.text])
            # tweet (idTweet bigint,text text,date date,iduser bigint)
        
        # writing author data
        with open(self.path_author, 'a', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([status.author.id, 
                             status.author.followers_count, 
                             status.author.friends_count, 
                             status.author.created_at])
            # user(iduser bigint, numFollowers int, numFriends int, createdAt date)
        
        # decreasing count variable
        if self.count != None:
            self.count -=1
            
            if self.count == 0:
                return False
            
    def on_error(self, status_code):
        """print an error code when an error occurs but let the stream continue"""
        print('Encountered error with status code:', status_code)
        return True # Don't kill the stream

# writing headings in both files
with open(path + 'trump_tweets.txt', 'w', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['tweet_id', 'user_id', 'tweet_date', 'text'])
with open(path + 'trump_tweets_author.txt', 'w', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['user_id', 'num_followers', 'num_friends', 'created_date'])

# creating a listener
listener = CustomStreamListener(count = 1000, verbose = 0, 
                                path_tweet=path + 'trump_tweets.txt',
                                path_author = path + 'trump_tweets_author.txt')

# launching stream
streamingAPI = tweepy.streaming.Stream(auth, listener)
streamingAPI.filter(track=['trump'])

