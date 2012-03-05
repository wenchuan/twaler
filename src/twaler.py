#!/usr/local/bin/python3
import os
import shutil
import sys
import signal
import multiprocessing
import json

import crawl
import process_crawl
import load_crawl
import generate_seeds
import misc

from misc import parse_arguments

class Twaler:
    # TODO fix all these parameter passing diaster
    def __init__(self, dir_cache, dir_log, dir_seeds, dir_seedsdone,
                 **kwargs):
        # load configurations from config.json
        fp = open('config.json')
        self.config = json.load(fp)
        fp.close()
        self.name = "twaler"
        self.configurations = kwargs
        self.dir_seeds = dir_seeds
        self.dir_seedsdone = dir_seedsdone
        # Create directory for cache and log if not already
        if not os.path.exists(dir_seedsdone):
            os.makedirs(dir_seedsdone)
        self.dir_cache = dir_cache
        if not os.path.exists(dir_cache):
            os.makedirs(dir_cache)
        self.dir_log = dir_log
        if kwargs['verbose']:
            self.log = misc.Logger(self.name, dir_log).verbose_log
        else:
            self.log = misc.Logger(self.name, dir_log).log

    def twale(self):
        self.child = os.fork()              # create a child process
        if self.child == 0:
            self.twalerloop()               # child works, father watches
        else:
            self.watch()

    def watch(self):
        try:
            os.wait()
        except (KeyboardInterrupt, SystemExit):
            self.log("Keyboard Interrupt Received")
            os.kill(self.child, signal.SIGKILL)
        sys.exit()

    def watchlist(self):
        '''Watch a fixed list of user_id'''
        # get that list first
        # get that list's friend second
        # enter plan-crawl-update loop
        import pdb; pdb.set_trace()
        while True:
            seeds = os.listdir(self.dir_seeds)

    def twalerloop(self):
        # Loop forever unless interrupted by user
        while True:
            # Check for seeds
            seeds = os.listdir(self.dir_seeds)
            # Generate more if needed
            if not seeds:
                self.log("Seed Folder Empty")
                self.generateseeds()
                self.log("Generate seeds complete")
                seeds = os.listdir(self.dir_seeds)
            for seed in seeds:          # N.B. seed is a filename with seeds
                self.crawl(seed)

    def generateseeds(self):
        self.configurations["instance"] = misc.timefunctions.datestamp()
        self.configurations["dir_seeds"] = self.dir_seeds
        self.configurations["dir_log"] = self.dir_log
        generator = generate_seeds.Generator(**self.configurations)
        generator.generate()

    def crawl(self, seed):
        '''Read seed file, download, process and load to database'''
        # A time stamp universally identified the crawling instance
        timestamp = misc.timefunctions.datestamp()
        # Set new parameters for crawling
        # A folder under cache will be created with the timestamp
        seed_cache_dir = os.path.join(self.dir_cache, timestamp)
        self.configurations["seed_file"] = os.path.join(self.dir_seeds, seed)
        self.configurations["dir_cache"] = seed_cache_dir
        self.configurations["instance"] = timestamp
        self.configurations["dir_log"] = os.path.join(seed_cache_dir, "log")
        # crawl the given instance
        self.log("Crawling " + seed)
        crawler = crawl.Crawler(**self.configurations)
        crawler.crawlloop()
        self.processAndLoad(timestamp)
        # Move seedfile out of seed directory
        seedpath = os.path.join(self.dir_seeds, seed)
        cachepath = os.path.join(self.dir_cache, timestamp,
                                 "seed["+seed+"].txt")
        seeddonepath = os.path.join(self.dir_seedsdone, seed)
        shutil.copy(seedpath, cachepath)
        shutil.move(seedpath, seeddonepath)

    def processAndLoad(self, timestamp):
        # A folder under cache will be created with the timestamp
        cache_dir = os.path.join(self.dir_cache, timestamp)
        self.configurations["dir_cache"] = cache_dir
        self.configurations["instance"] = timestamp
        self.configurations["dir_log"] = os.path.join(cache_dir, "log")
        self.configurations["dir_processed"] = (
                os.path.join(cache_dir, "processed_crawl"))
        # PROCESS
        self.log("Processing instance " + timestamp)
        processor = process_crawl.Process_crawl(**self.configurations)
        processor.process_loop()
        # LOAD
        self.log("Loading instance " + timestamp)
        loader = load_crawl.Load_crawl(**self.configurations)
        loader.load_loop()


def usage():
    print("""\nUsage: %s [manual parameters] <config file>\n
Contents in configuration file:
*Values listed here are default values, used if parameter is unspecified
*Can also be overwritten with parameters
*ie) %s --seed_file=seedfile.txt config.txt

dir_seeds= seeds
dir_seedsdone= seedsdone
dir_cache= cache
dir_log= log
verbose = 0

#---crawl---#
crawl_num_of_threads= 10

#---process_crawl---#
process_userinfo= 1
process_friends= 1
process_memberships= 1
process_tweets= 1
process_listmembers= 1
extract_mentions= 1
extract_urls= 1
extract_hashes=1

#---load_crawl---#
db_server= localhost
db_database= twaler
db_username= snorgadmin
db_password= snorg321

#---generate_seeds---#
seed_userinfo = 1
seed_tweets= 1
seed_friends= 1
seed_listmemberships= 1
seed_lists= 1
seed_per_file = 200
seed_limit = 2000
update_limit = 2000
list_limit = 100
        """ % (sys.argv[0],sys.argv[0]))

if __name__ == '__main__':
    parameters = {"dir_cache":"cache","dir_log":"log", "dir_seeds":"seeds","dir_seedsdone":"seedsdone", "verbose":0}
    int_params = ["verbose"]
    #parameters from crawl
    parameters.update({"seed_file":"<in dir_seeds>","crawl_num_of_threads":10})
    int_params.extend(["crawl_num_of_threads"])
    #parameters from process_crawl
    parameters.update({"process_to_db":0,"dir_processed":"<in cache instance folders>","process_userinfo":1,"process_friends":1,"process_memberships":1,"process_tweets":1,"process_listmembers":1,"extract_mentions":1,"extract_urls":1,"extract_hashes":1})
    int_params.extend(["process_userinfo","process_friends","process_memberships",  "process_tweets","process_listmembers","extract_mentions","extract_urls","extract_hashes"])
    #parameters from load_crawl
    parameters.update({"db_server":"localhost","db_database":"twaler","db_username":"snorgadmin","db_password":"snorg321"})
    #parameters from generate_seeds
    parameters.update({"seed_userinfo":1,"seed_tweets":1,"seed_friends":1,"seed_listmemberships":1,"seed_lists":1,"seed_per_file":200,"seed_limit":2000,"update_limit":2000,"list_limit":100})
    int_params.extend(["seed_userinfo","seed_tweets","seed_friends","seed_listmemberships","seed_lists","seed_per_file","seed_limit","update_limit","list_limit"])
    conf = parse_arguments(usage,parameters,int_params);
    t = Twaler(**conf)
    t.twale()
