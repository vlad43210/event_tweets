'''parser for event-related tweets
using format provided by Chris Cassa and Megan Williams'''

class TweetParser(object):
    def __init__():
        pass
                
    def parse_file(tweet_file_name):
        tweet_file = open(tweet_file_name,'r')
        tweet_network_hash = {}
        for line in tweet_file:
            #assume file is csv
            tweet_objs = line.split(",")
            
        
        tweet_file.close()