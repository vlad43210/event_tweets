'''parser for event-related tweets
using format provided by Chris Cassa and Megan Williams'''
import time

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
    
    #parse tweet file with classes into ARFF file for Weka processing
    def parse_file_arff(self,tweet_file_name):
        dataset_name = tweet_file_name.rstrip(".txt")
        arff_file = open(dataset_name+".arff","w")
        #comments
        arff_file.write("%% %s\n" %(dataset_name))
        #relation name
        arff_file.write("@RELATION %s\n" %(dataset_name.split("/")[-1]))
        #blank space
        arff_file.write("\n")
        #attribute_names
        arff_file.write("@ATTRIBUTE created_at_tstamp NUMERIC\n")
        arff_file.write("@ATTRIBUTE created_at_dayofweek NUMERIC\n")
        arff_file.write("@ATTRIBUTE created_at_hour NUMERIC\n")                
        arff_file.write("@ATTRIBUTE favorited {TRUE,FALSE}\n")
        arff_file.write("@ATTRIBUTE in_reply_to STRING\n")
        arff_file.write("@ATTRIBUTE lang STRING\n")
        arff_file.write("@ATTRIBUTE source STRING\n")
        arff_file.write("@ATTRIBUTE text STRING\n")
        arff_file.write("@ATTRIBUTE user_profile_desc STRING\n")               
        arff_file.write("@ATTRIBUTE user_profile_loc STRING\n")
        arff_file.write("@ATTRIBUTE user_created_at_tstamp NUMERIC\n")
        arff_file.write("@ATTRIBUTE user_followers_count NUMERIC\n")
        arff_file.write("@ATTRIBUTE user_name STRING\n")
        arff_file.write("@ATTRIBUTE user_screen_name STRING\n")
        arff_file.write("@ATTRIBUTE user_time_zone STRING\n")
        arff_file.write("@ATTRIBUTE tweet_class {POSITIVE,NEGATIVE}\n")  
        arff_file.write("@DATA\n")      
        tweet_file = open(tweet_file_name,'r')
        tweet_network_hash = {}
        #print "num lines: ", len(tweet_file.read().split("\r"))
        #print "test lines: ", tweet_file.read().split("\r")[0:3]
        lctr = 0
        for line in tweet_file.read().split("\r"):
            lctr+=1
            if lctr%100 == 0: print "line ctr: ", lctr
            #assume file is tsv
            tweet_objs = line.split("\t")
            if len(tweet_objs) != 15: 
                print "badly formatted line: ", line
                continue
            #lines at beginning that don't have commas
            created_at = tweet_objs[0]
            #convert created at to iso 8601 format
            if not created_at:
                created_at_tstamp = 0
                created_at_dayofweek = 0
                created_at_hour = 0
            else:
                created_at_tstruct = time.strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y")
                created_at_tstamp = time.mktime(created_at_tstruct)
                created_at_dayofweek = created_at_tstruct[6]
                created_at_hour = created_at_tstruct[3]
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
            user_profile_loc = tweet_objs[8]
            user_profile_loc = "\"" + user_profile_loc.strip("\"") + "\""
            if tweet_objs[9]:
                user_created_at_tstruct = time.strptime(tweet_objs[9], "%a %b %d %H:%M:%S +0000 %Y")
                user_created_at_tstamp = time.mktime(user_created_at_tstruct)
            else:
                user_created_at_tstamp = 0
            user_followers_count = tweet_objs[10]
            user_name = tweet_objs[11]
            user_name = "\"" + user_name.strip("\"") + "\""
            user_screen_name = tweet_objs[12]
            user_time_zone = tweet_objs[13]
            user_time_zone = "\"" + user_time_zone.strip("\"") + "\""
            tweet_class = tweet_objs[14]
            arff_file.write(",".join([str(x) for x in [created_at_tstamp, created_at_dayofweek, created_at_hour, favorited, in_reply_to, \
                                                      lang, source, text, user_profile_desc, user_profile_loc, user_created_at_tstamp, \
                                                      user_followers_count, user_name, user_screen_name, user_time_zone, tweet_class]])+"\n")
        tweet_file.close()
        arff_file.close()
            
if __name__ == '__main__':
    tp = TweetParser()
    tp.parse_file_arff("/Users/vdb5/Documents/work/projects/chris cassa collaboration/all_instances_seattle.txt")
