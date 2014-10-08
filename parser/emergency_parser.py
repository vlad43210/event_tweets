import math, time

class EmergencyParser(object):
    def __init__(self, em_file_name):
        self.em_file_name = em_file_name
        self.dataset_name = self.em_file_name.rstrip(".txt")
        self.base_dir = "/".join(self.tweet_file_name.split("/")[:-1])
        
    def parse_emergencies_with_tweets(self, tweet_file_name):
        #first, get tweets by timestamp from tweet file:
        tweet_file = open(tweet_file_name,'r')
        tweets_by_ts = {}
        #hash tstamps by hour for easy identification
        tstamps_by_hour_hash = {}        
        lctr = 0
        for line in tweet_file:
            lctr+=1
            #skip first line
            if lctr==1: continue
            els = line.split(",")
            source = els[0]
            tar = els[1]
            tstamp = float(els[5])
            tstamp_hour = tstamp/3600
            tstamps_by_hour_hash[tstamp_hour] = tstamps_by_hour_hash.get(tstamp_hour,[]) + [tstamp]
            text = els[8]
            tweets_by_ts[tstamp] = {"source":source,"tar":tar,"text":text,"tstamp":tstamp}
        tweet_file.close()
        for 
        #now iterate through emergency file and attach time-near tweets to each emergency
        em_file = open(self.em_file_name,'r')
        em_hash_by_inc_num = {}
        em_lctr = 0
        for line in em_file:
            em_lctr+=1
            #skip first line
            if em_lctr==1: continue
            address,em_type,em_datetime,em_lat,em_long,em_loc,inc_num = line.split("\t")
            #get rid of AM/PM, tzone
            clean_em_datetime = ",".join(em_datetime.split(" ")[:-2])
            #extract timestamp:
            em_tstruct = time.strptime(clean_em_datetime, "%d/%m/%Y %H:%M:%S +0000 %Y")            
            em_tstamp = time.mktime(em_tstruct)
            em_tstamp_hr = em_tstamp/3600
            #fill out hash
            em_hash_by_inc_num[inc_num] = {"address":address,"type":em_type,"tstamp":em_tstamp,"loc":em_loc,"cand_tweets":[]}
            #find candidate tweet timestamps +- 1 hour bin
            candidate_tweet_tstamps = tstamps_by_hour_hash[tstamp_hour-1] + tstamps_by_hour_hash[tstamp_hour] + \
                                      tstamps_by_hour_hash[tstamp_hour+1]            
            #find nearby tweets: +- .5 hours and append to record
            for ctt in candidate_tweet_tstamps:
                if math.abs(ctt-em_tstamp) < 1800:
                    em_hash.append(tweets_by_tsp[ctt])                                    
        em_file.close()
        #finally, store near tweets in a file
        em_near_tweets_file = open(self.dataset_name+"_near_tweets.txt","w")
        for em_inc_num,em_meta in em_hash_by_inc_num.items():
            em_near_tweets_file.write("NEW EMERGENCY")
            em_near_tweets_file.write("%s,%s,%s,%f,%s\n" %(em_inc_num,em_meta["address"],em_meta["type"],em_meta["tstamp"],em_meta["loc"]))
            for cand_tweet in em_meta["cand_tweets"]:
                em_near_tweets_file.write("%s,%s,%s,%f\n" %(cand_tweet["source"],cand_tweet["tar"],cand_tweet["text"],cand_tweet["tstamp"]))
        em_near_tweets_file.close()
        
    