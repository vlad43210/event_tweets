'''parser for event-related tweets
using format provided by Chris Cassa and Megan Williams'''
import time

class TweetParser(object):
    def __init__(self, tweet_file_name):
        self.tweet_file_name = tweet_file_name
        self.dataset_name = self.tweet_file_name.rstrip(".txt")
        self.base_dir = "/".join(self.tweet_file_name.split("/")[:-1])
                
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
    def parse_file_arff(self,test_file=False):
        arff_file = open(self.dataset_name+".arff","w")
        #comments
        arff_file.write("%% %s\n" %(self.dataset_name))
        #relation name
        arff_file.write("@RELATION %s\n" %(self.dataset_name.split("/")[-1]))
        #blank space
        arff_file.write("\n")
        #attribute_names
        arff_file.write("@ATTRIBUTE created_at_tstamp NUMERIC\n")
        arff_file.write("@ATTRIBUTE created_at_dayofweek NUMERIC\n")
        arff_file.write("@ATTRIBUTE created_at_hour NUMERIC\n")                
        #arff_file.write("@ATTRIBUTE favorited {TRUE,FALSE}\n")
        #arff_file.write("@ATTRIBUTE in_reply_to STRING\n")
        #arff_file.write("@ATTRIBUTE lang STRING\n")
        #arff_file.write("@ATTRIBUTE source STRING\n")
        #arff_file.write("@ATTRIBUTE text STRING\n")
        #arff_file.write("@ATTRIBUTE user_profile_desc STRING\n")               
        #arff_file.write("@ATTRIBUTE user_profile_loc STRING\n")
        arff_file.write("@ATTRIBUTE user_created_at_tstamp NUMERIC\n")
        arff_file.write("@ATTRIBUTE user_followers_count NUMERIC\n")
        #arff_file.write("@ATTRIBUTE user_name STRING\n")
        #arff_file.write("@ATTRIBUTE user_screen_name STRING\n")
        #arff_file.write("@ATTRIBUTE user_time_zone STRING\n")
        for top_token in self.top_token_list:
            arff_file.write("@ATTRIBUTE %s {0,1}\n" %(top_token))
        #for test file, tweet class is unknown
        if test_file:
            arff_file.write("@ATTRIBUTE tweet_class {POSITIVE,NEGATIVE,?}\n")  
        else:
            arff_file.write("@ATTRIBUTE tweet_class {POSITIVE,NEGATIVE}\n")  
        arff_file.write("@DATA\n")      
        tweet_file = open(self.tweet_file_name,'r')
        tweet_network_hash = {}
        #print "num lines: ", len(tweet_file.read().split("\r"))
        #print "test lines: ", tweet_file.read().split("\r")[0:3]
        lctr = 0
        for line in tweet_file.read().split("\r"):
            lctr+=1
            #if lctr%100 == 0: print "line ctr: ", lctr
            #assume file is tsv
            tweet_objs = line.split("\t")
            if len(tweet_objs) != 15: 
                #print "badly formatted line: ", line
                continue
            #empty data
            if len([x for x in tweet_objs if len(x) > 0]) < 10:
                #print "badly formatted line: ", line
                continue
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
            if not in_reply_to:
                in_reply_to = "None"
            lang = tweet_objs[3]
            permalink = tweet_objs[4]
            source = tweet_objs[5]
            #add quote marks to text fields, but strip quote marks first
            source = "\"" + source.replace("\"","").replace(",","") + "\""
            text = tweet_objs[6]
            raw_text = text.replace("\"","").replace(",","").replace("\\","").replace("'","")
            raw_tokens = raw_text.split()
            raw_token_vector = [0]*len(self.top_token_list)            
            for raw_token in raw_tokens:
                if raw_token in self.top_tokens:
                    raw_token_vector[self.top_token_list.index(raw_token)] = 1
            text = "\"" + text.replace("\"","").replace(",","").replace("\\","").replace("'","") + "\""            
            user_profile_desc = tweet_objs[7]
            user_profile_desc = "\"" + user_profile_desc.replace("\"","").replace(",","").replace("\\","") + "\""
            if len(user_profile_desc) < 5:
                user_profile_desc = "\"None\""
            user_profile_loc = tweet_objs[8]
            user_profile_loc = "\"" + user_profile_loc.replace("\"","").replace(",","").replace("\\","") + "\""
            if tweet_objs[9]:
                user_created_at_tstruct = time.strptime(tweet_objs[9], "%a %b %d %H:%M:%S +0000 %Y")
                user_created_at_tstamp = time.mktime(user_created_at_tstruct)
            else:
                user_created_at_tstamp = 0
            user_followers_count = tweet_objs[10]
            user_name = tweet_objs[11]
            user_name = "\"" + user_name.replace("\"","").replace(",","").replace("\\","") + "\""
            user_screen_name = tweet_objs[12]
            user_time_zone = tweet_objs[13]
            user_time_zone = "\"" + user_time_zone.replace("\"","").replace(",","") + "\""
            #for test file, tweet class unknown
            if test_file:
                tweet_class = "?"
            else:
                tweet_class = tweet_objs[14]
            #arff_file.write(",".join([str(x) for x in [created_at_tstamp, created_at_dayofweek, created_at_hour, \
            #                          favorited, in_reply_to, lang, source, text, user_profile_desc, \
            #                          user_profile_loc,user_created_at_tstamp, user_followers_count, \
            #                          user_name, user_screen_name, user_time_zone, tweet_class]])+"\n")
            arff_file.write(",".join([str(x) for x in [created_at_tstamp, created_at_dayofweek, created_at_hour, \
                                      user_created_at_tstamp, user_followers_count] + raw_token_vector + \
                                      [tweet_class]])+"\n")
        tweet_file.close()
        arff_file.close()
        
    def get_stop_words(self):
        self.stop_words_list = ["&amp;","..."]
        stop_words_file = open(self.base_dir+"/stop_words.txt")
        for line in stop_words_file:
            self.stop_words_list.append(line.strip())
        stop_words_file.close()
        
    def token_distribution(self):
        self.top_token_cutoff = 500
        tweet_file = open(self.tweet_file_name,'r')        
        token_file = open(self.dataset_name+".tokens","w")
        token_hash = {}
        lctr = 0
        for line in tweet_file.read().split("\r"):
            lctr+=1
            #if lctr%100 == 0: print "line ctr: ", lctr
            #assume file is tsv
            tweet_objs = line.split("\t")
            text = tweet_objs[6]
            text = text.replace("\"","").replace(",","").replace("\\","").replace("'","")
            tokens = text.split()
            for token in tokens:
                if len(token) > 1 and token not in self.stop_words_list:
                    token_hash[token] = token_hash.get(token,0)+1
        tweet_file.close()
        sorted_tokens = sorted(token_hash.items(), key = lambda x: x[1], reverse=True)
        self.top_tokens = dict(sorted_tokens[:self.top_token_cutoff])
        self.top_token_list = [x[0] for x in sorted_tokens][:self.top_token_cutoff]
        for st in sorted_tokens:
            token_file.write("%s,%i\n" %(st[0],st[1]))            
        token_file.close()
            
if __name__ == '__main__':
    #tweet_file_name = "/Users/vdb5/Documents/research/real world tweets/all_instances_seattle.txt"
    #tp = TweetParser(tweet_file_name)
    #tp.get_stop_words()
    #tp.token_distribution()
    #tp.parse_file_arff(test_file=False)
    #parse test file
    test_tweet_file_name = "/Users/vdb5/Documents/research/real world tweets/Seattle raw tweets.txt"
    tp = TweetParser(tweet_file_name)
    tp.get_stop_words()
    tp.token_distribution()
    tp.parse_file_arff(test_file=True)
    
