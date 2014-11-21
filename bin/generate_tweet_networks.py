'''script for generating tweet networks from Seattle data'''

import os, sys, time, warnings
_root_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, _root_dir)
from parser.tweet_parser import TweetParser
from network.network_builder import NetworkBuilder

def generate_tweet_networks():
    tp = TweetParser()
    tweet_file_name = "/Users/vdb5/Documents/research/real world tweets/Seattle raw tweets.txt"
    network_hash = tp.parse_file(tweet_file_name)
    print "num sources: ", len(network_hash)
    print "sample source: ", network_hash.items()[0]
    nb = NetworkBuilder(network_hash, tweet_file_name)
    nb.store_network_hash_as_edgefile()
    
if __name__ == '__main__':
    generate_tweet_networks()