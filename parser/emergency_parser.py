import datetime, math, pytz, time

class EmergencyParser(object):
    def __init__(self, em_file_name):
        self.em_file_name = em_file_name
        self.dataset_name = self.em_file_name.rstrip(".txt")
        self.base_dir = "/".join(self.em_file_name.split("/")[:-1])
        self.timezone = pytz.timezone("US/Pacific")
        
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
            tstamp = int(float(els[5]))            
            tstamp_hour = int(tstamp/3600)
            #if tstamp == 1357369999:
            #    print "tstamp: ", tstamp, " hour: ", tstamp_hour
            tstamps_by_hour_hash[tstamp_hour] = tstamps_by_hour_hash.get(tstamp_hour,[]) + [tstamp]
            text = els[8]
            tweets_by_ts[tstamp] = {"source":source,"tar":tar,"text":text,"tstamp":tstamp}
        #print "test stamps by hour hash: ", tstamps_by_hour_hash[377047]
        tweet_file.close()
        #print "number of tweet timestamps: ", len(tweets_by_ts)
        #now iterate through emergency file and attach time-near tweets to each emergency
        em_file = open(self.em_file_name,'r')
        em_hash_by_inc_num = {}
        em_lctr = 0
        for line in em_file.read().split("\r"):
            em_lctr+=1
            #ignore mal-formatted lines
            if len(line) < 20: continue
            if em_lctr%50000==0: print "on em line: ", em_lctr, " at: ", time.time()
            #skip first line
            if em_lctr==1: continue
            address,em_type,em_datetime,em_lat,em_long,em_loc,inc_num = line.split("\t")
            if len(address.split()) < 2:
                em_street = " %s" %(address)
            else:
                em_street = " %s" %(" ".join(address.split()[1:]))
            #if em_lctr < 100:
            #    print "em street: ", em_street
            #get rid of AM/PM, tzone
            clean_em_datetime = ",".join(em_datetime.split(" ")[:-1])
            #deal with AM/PM dist
            #extract timestamp:
            try:
                t_format_str = "%m/%d/%Y,%H:%M:%S,%p"
                em_tstruct = time.strptime(clean_em_datetime, t_format_str)            
            except Exception, e:
                #print e.message
                #print "line: ", line, " lctr: ", em_lctr
                #if em datetime empty, just ignore it
                #if not em_datetime or em_datetime == " ": continue
                #try alternate format:
                clean_em_datetime = em_datetime[:-5].lstrip()
                clean_date = clean_em_datetime.split("T")[0]
                clean_time = clean_em_datetime.split("T")[1]
                clean_time_non_hr = ":".join(clean_time.split(":")[1:])
                clean_hr = clean_em_datetime.split("T")[1].split(":")[0]
                #get rid of 24 as hour!
                if clean_hr == "24":
                    clean_hr = "00"
                clean_em_datetime = clean_date + "," + clean_hr + ":" + clean_time_non_hr
                t_format_str = "%Y-%m-%d,%H:%M:%S"
                em_tstruct = time.strptime(clean_em_datetime, t_format_str) 
            #localize time
            em_tstamp = time.mktime(em_tstruct)-18000
            em_tstamp_hr = int(em_tstamp/3600)
            #fill out hash
            em_hash_by_inc_num[inc_num] = {"address":address,"type":em_type,"tstamp":em_tstamp,"loc":em_loc,"cand_tweets":[]}
            #find candidate tweet timestamps +- 1 hour bin
            candidate_tweet_tstamps = tstamps_by_hour_hash.get(em_tstamp_hr-3,[]) + tstamps_by_hour_hash.get(em_tstamp_hr-2,[]) + \
                                      tstamps_by_hour_hash.get(em_tstamp_hr-1,[]) + tstamps_by_hour_hash.get(em_tstamp_hr,[]) + \
                                      tstamps_by_hour_hash.get(em_tstamp_hr+1,[]) + tstamps_by_hour_hash.get(em_tstamp_hr+2,[]) + \
                                      tstamps_by_hour_hash.get(em_tstamp_hr+3,[])
            #if clean_em_datetime == "01/05/2013,07:19:00,AM":                
            #    print "clean time: ", clean_em_datetime, "struct: ", em_tstruct, "tstamp: ", em_tstamp, " hour: ", em_tstamp_hr
            #    print "candidates: ", candidate_tweet_tstamps  
            #    print "stamps by hour: ", tstamps_by_hour_hash[em_tstamp_hr] 
            #if an emergency lacks an address, ignore it (but only down here, so we do make a record)
            if len(em_street)<3: continue
            #find nearby tweets: any that have overlapping address and append to record
            for ctt in candidate_tweet_tstamps:
                cand_text = tweets_by_ts[ctt]["text"]
                if em_street in cand_text:
                    em_hash_by_inc_num[inc_num]["cand_tweets"].append(tweets_by_ts[ctt])
                #if abs(ctt-em_tstamp) < 1800:
                #    em_hash_by_inc_num[inc_num]["cand_tweets"].append(tweets_by_ts[ctt])                                    
        em_file.close()
        #finally, store near tweets in a file
        #also get some aggregate stats
        em_near_tweets_file = open(self.dataset_name+"_near_tweets.txt","w")
        em_near_tweets_stats_file = open(self.dataset_name+"_near_tweets_stats.txt","w")
        total_ems = 0
        ems_with_candidates = 0
        ems_with_earlier_candidates = 0
        ems_with_earlier_candidates_by_type = {}
        ems_with_earlier_candidates_by_date = {}
        ems_with_earlier_candidates_by_loc = {}
        for em_inc_num,em_meta in em_hash_by_inc_num.items():
            total_ems+=1
            if len(em_meta["cand_tweets"]) > 0:
                ems_with_candidates+=1
                em_near_tweets_file.write("NEW EMERGENCY")
                em_near_tweets_file.write("%s,%s,%s,%f,%s\n" %(em_inc_num,em_meta["address"],em_meta["type"],em_meta["tstamp"],em_meta["loc"]))
                has_earlier_candidates = False
                for cand_tweet in em_meta["cand_tweets"]:                    
                    em_near_tweets_file.write("%s,%s,%s,%f\n" %(cand_tweet["source"],cand_tweet["tar"],cand_tweet["text"],cand_tweet["tstamp"]))
                    if int(cand_tweet["tstamp"]) < int(em_meta["tstamp"]):
                        has_earlier_candidates = True
                if has_earlier_candidates:
                    ems_with_earlier_candidates+=1
                    if em_meta["type"]:
                        ems_with_earlier_candidates_by_type[em_meta["type"]] = ems_with_earlier_candidates_by_type.get(em_meta["type"],0)+1
                    em_meta_date_struct = time.gmtime(em_meta["tstamp"]-18000)
                    em_meta_date = "%s_%s" %(em_meta_date_struct[0], em_meta_date_struct[7])
                    ems_with_earlier_candidates_by_date[em_meta_date] = ems_with_earlier_candidates_by_date.get(em_meta_date,0)+1
                    if em_meta["loc"]:
                        loc_rounder = lambda x: int(round(x,2)*100)
                        clean_loc = [float(x) for x in em_meta["loc"].strip("\"").strip(")").strip("(").split(",")]
                        rough_em_meta_loc = (loc_rounder(clean_loc[0]),loc_rounder(clean_loc[1]))
                        ems_with_earlier_candidates_by_loc[rough_em_meta_loc] = ems_with_earlier_candidates_by_loc.get(rough_em_meta_loc,0)+1
        em_near_tweets_file.close()
        sorted_date_items = sorted(ems_with_earlier_candidates_by_date.items(), key=lambda x:int(x[0].split("_")[0])+int(x[0].split("_")[1]))
        sorted_loc_items = sorted(ems_with_earlier_candidates_by_loc.items(), key=lambda x:int(x[0][0])+int(x[0][1]))
        ems_with_ec_bt_str = "\n".join(["%s,%i" %(k,v) for k,v in ems_with_earlier_candidates_by_type.items()])
        ems_with_ec_bd_str = "\n".join(["%s,%i" %(k,v) for k,v in sorted_date_items])
        ems_with_ec_bl_str = "\n".join(["%i_%i,%i" %(k[0],k[1],v) for k,v in sorted_loc_items])
        em_near_tweets_stats_file.write("total,%i\nwith_candidates,%i\nearlier,%i\nearlier_by_type\n%s\nearlier_by_date\n%s\nearlier_by_loc\n%s\n" \
                                       %(total_ems, ems_with_candidates, ems_with_earlier_candidates, ems_with_ec_bt_str, ems_with_ec_bd_str, \
                                       ems_with_ec_bl_str))
        em_near_tweets_stats_file.close()
        
if __name__ == '__main__':
    #ep = EmergencyParser("/Users/vdb5/Documents/research/real world tweets/Seattle_Real_Time_Fire_911_Calls.txt")
    #ep.parse_emergencies_with_tweets("/Users/vdb5/Documents/research/real world tweets/Seattle_raw_tweets_candidates_nodexl.csv")
    ep = EmergencyParser("/Users/vdb5/Documents/work/projects/chris cassa collaboration/Seattle_Real_Time_Fire_911_Calls.txt")
    ep.parse_emergencies_with_tweets("/Users/vdb5/Documents/work/projects/chris cassa collaboration/Seattle_raw_tweets_candidates_precise_nodexl.csv")
        
    