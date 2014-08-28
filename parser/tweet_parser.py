'''parser for event-related tweets
using format provided by Chris Cassa and Megan Williams'''
import time

class TweetParser(object):
    def __init__():
        pass
    
    #parse tweet file with classes into ARFF file for Weka processing
    def parse_file_arff(tweet_file_name):
        tweet_file = open(tweet_file_name,'r')
        tweet_network_hash = {}
        for line in tweet_file:
            #assume file is tsv
            tweet_objs = line.split("\t")
            #lines at beginning that don't have commas
            created_at = tweet_objs[0]
            #convert created at to iso 8601 format
            created_at_tstruct = time.strptime("%a %B %d %H:%M:S +0000 %Y", created_at)
            created_at_tstamp = time.gmtime(created_at_tstruct)
            created_at_dayofweek = created_at_tstruct[6]
            created_at_hour = tstruct[3]
            favorited = tweet_objs[1]
            in_reply_to = tweet_objs[2]
            lang = tweet_objs[3]
            permalink = tweet_objs[4]
            source = tweet_objs[5]
            #add quote marks to text fields, but strip quote marks first
            source = "\"" + source.strip("\"") + "\""
            text = tweet_objs[6]
            text = "\"" + text.strip("\"") + "\""
            user_profile_desc = tweet_objs[7]
            user_profile_desc = "\"" + user_profile_desc.strip("\"") + "\""
            user_profile_loc = tweet_objs[7]
            user_profile_loc = "\"" + user_profile_loc.strip("\"") + "\""
            user_created_at_tstruct = time.strptime("%a %B %d %H:%M:S +0000 %Y", tweet_objs[8])
            user_created_at_tstamp = time.gmtime(user_created_at_tsruct)
            user_followers_count = tweet_objs[9]
            user_name = tweet_objs[10]
            user_name = "\"" + user_name.strip("\"") + "\""
            user_screen_name = tweet_objs[11]
            user_time_zone = tweet_objs[12]
            user_time_zone = "\"" + user_time_zone.strip("\"") + "\""
            tweet_class = tweet_objs[13]
        tweet_file.close()
    
    def parse_file(tweet_file_name):
        tweet_file = open(tweet_file_name,'r')
        tweet_network_hash = {}
        for line in tweet_file:
            #assume file is csv
            tweet_objs = line.split(",")
            #lines at beginning that don't have commas
            created_at = tweet_objs[0]
            favorited = tweet_objs[1]
            in_reply_to = tweet_objs[2]
            lang = tweet_objs[3]
            permalink = tweet_objs[4]
            source = tweet_objs[5]
            #lines at end that don't have commas
            user_time_zone = tweet_objs[-1]
            user_screen_name = tweet_objs[-2]
            #the rest of the stuff has commas maybe
            #this chunk has text, user profile description, user pfofile location, user_created_at, user_followers_count, user_name
            chunk_with_commas = ",".join(tweet_objs[6:-2])
            #tweet text has commas
            if chunk_with_commas.startswith("\""):
                chunk_split_by_quote = chunk_with_commas.split("\"")
        tweet_file.close()