'''parser for event-related tweets
using format provided by Chris Cassa and Megan Williams'''

class TweetParser(object):
    def __init__(self):
        pass
                
    def parse_file(self, tweet_file_name):
        '''parse edge file to return Python network hash'''
        tweet_file = open(tweet_file_name,'r')
        tweet_network_hash = {}
        for line in tweet_file.read().split("\r"):
            #some lines empty
            if not line: continue
            #assume file is tsv
            try:
                [created_at, favorited, in_reply_to_screen_name, lang, permanent_link, source, text, \
            user_profile_description, user_profile_location, user_created_at, user_followers_count, user_name, \
            user_screen_name, user_time_zone, blank] = line.split("\t")            
            except Exception, e:
                #sometimes no blank?
                try:
                    [created_at, favorited, in_reply_to_screen_name, lang, permanent_link, source, text, \
                user_profile_description, user_profile_location, user_created_at, user_followers_count, user_name, \
                user_screen_name, user_time_zone] = line.split("\t")
                except Exception, e:
                    #corrupted line, extra tab, etc.
                    continue
            source_sn = user_screen_name.lstrip("@")
            #blank source screen names
            if not source_sn: continue
            #reply edge
            if in_reply_to_screen_name:
                target_sn = in_reply_to_screen_name.lstrip("@")
                edge_type = "reply"
            #retweet edge
            elif text.startswith("RT "):
                target_sn = text.split(" ")[1].lstrip("@")
                edge_type = "retweet"
            #blank target screen names
            if not target_sn: continue
            #store edge            
            if source_sn in tweet_network_hash:
                if target_sn in tweet_network_hash[source_sn]:
                    tweet_network_hash[source_sn][target_sn].append((edge_type, text, created_at))
                else:
                    tweet_network_hash[source_sn][target_sn] = [(edge_type, text, created_at)]
            else:
                tweet_network_hash[source_sn] = {target_sn:[(edge_type, text, created_at)]}        
        tweet_file.close()
        return tweet_network_hash