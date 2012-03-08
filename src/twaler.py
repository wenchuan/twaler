#!/usr/local/bin/python3
import os
import shutil
import sys
import signal
import json
import logging
import logging.handlers

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
        # Load configurations from config.json
        #----------------
        fp = open('config.json')
        self.config = json.load(fp)
        fp.close()

        # Setup logger
        #----------------
        formatter = logging.Formatter(
                '%(asctime)-6s: %(funcName)s(%(filename)s:%(lineno)d) - '
                '%(levelname)s - %(message)s')
        consoleLogger = logging.StreamHandler()
        consoleLogger.setLevel(logging.INFO)
        consoleLogger.setFormatter(formatter)
        logging.getLogger('').addHandler(consoleLogger)
        # Setup rotating log files
        log_dir = self.config['log_dir']
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_name = os.path.join(log_dir, self.config['log_name'])
        fileLogger = logging.handlers.RotatingFileHandler(
                filename=log_name, maxBytes = 1024*1024,
                backupCount = 50)
        fileLogger.setLevel(logging.DEBUG)
        fileLogger.setFormatter(formatter)
        logging.getLogger('').addHandler(fileLogger)
        self.logger = logging.getLogger('')
        self.logger.setLevel(logging.DEBUG)

        self.logger.debug('twaler started')

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

    def twale(self):
        self.child = os.fork()              # create a child process
        if self.child == 0:
            # self.twalerloop()               # child works, father watches
            self.watchlist()
        else:
            self.watch()

    def watch(self):
        try:
            os.wait()
        except (KeyboardInterrupt, SystemExit):
            self.logger.info("Keyboard Interrupt Received")
            os.kill(self.child, signal.SIGKILL)
        sys.exit()

    def watchlist(self):
        '''Watch a fixed list of user_id'''
        # Clean up database first
        db = misc.mysql_db(self.config['db_server'],
                           self.config['db_username'],
                           self.config['db_password'],
                           self.config['db_database'], self.logger)
        stmt = 'DELETE FROM target_users'
        db.execute(stmt)
        # TODO still not working correctly
        stmt = 'LOAD DATA LOCAL INFILE "seed.lst" INTO TABLE target_users'
        db.execute(stmt)
        # Get that list first
        self.crawl('seed.lst')
        # Get that list's friend second
        stmt = ('SELECT DISTINCT friend_id FROM friends, target_users '
                'WHERE friends.user_id = target_users.user_id')
        db.execute(stmt)
        results = db.cursor.fetchall()
        db.__del__()
        misc.write_to_files(results, 'initial_friends',
                            self.config['seed_per_file'], 'utf')
        # Enter the generate-crawl-update loop
        self.twalerloop()

    def twalerloop(self):
        # Loop forever unless interrupted by user
        while True:
            # Check for seeds
            seeds = os.listdir(self.dir_seeds)
            # Generate more if needed
            if not seeds:
                self.logger.debug("Seed folder empty")
                #________________________________
                #
                # import pdb; pdb.set_trace()
                #
                #________________________________
                self.generateseeds()
                self.logger.debug('Generate seeds complete')
                seeds = os.listdir(self.dir_seeds)
            for seed in seeds:          # N.B. seed is a filename with seeds
                self.crawl(seed)

    def generateseeds(self):
        self.configurations["instance"] = misc.timefunctions.datestamp()
        self.configurations["dir_seeds"] = self.dir_seeds
        self.configurations["dir_log"] = self.dir_log
        generator = generate_seeds.Generator(self.config, self.logger)
        generator.generate()

    def crawl(self, seed):
        '''Read seed file, download, process and load to database
        A time stamp universally identified the crawling instance'''
        timestamp = misc.timefunctions.datestamp()
        self.logger.info('Crawling file %s as %s' % (seed, timestamp))

        cache_dir = os.path.join(self.dir_cache, timestamp)
        seedfile = os.path.join(self.dir_seeds, seed)

        # crawl seedfile and save files into cache_dir
        crawler = crawl.Crawler(seedfile, cache_dir, self.config, self.logger)
        crawler.crawlloop()
        self.processAndLoad(timestamp)

        # Move seedfile out of seed directory
        cachepath = os.path.join(self.dir_cache, timestamp,
                                 "seed["+seed+"].txt")
        seeddonepath = os.path.join(self.dir_seedsdone, seed)
        shutil.copy(seedfile, cachepath)
        shutil.move(seedfile, seeddonepath)

    def processAndLoad(self, timestamp):
        # A folder under cache will be created with the timestamp
        cache_dir = os.path.join(self.dir_cache, timestamp)
        self.configurations["dir_cache"] = cache_dir
        self.configurations["instance"] = timestamp
        self.configurations["dir_log"] = os.path.join(cache_dir, "log")
        self.configurations["dir_processed"] = (
                os.path.join(cache_dir, "processed_crawl"))
        # PROCESS
        self.logger.info("Processing instance " + timestamp)
        processor = process_crawl.Process_crawl(**self.configurations)
        processor.process_loop()
        # LOAD
        self.logger.info("Loading instance " + timestamp)
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
