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

class Twaler:
    def __init__(self):
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

        # Create directory for cache and log if not already
        #---------------
        dir_seedsdone = self.config['dir_seedsdone']
        dir_cache = self.config['dir_cache']
        if not os.path.exists(dir_seedsdone):
            os.makedirs(dir_seedsdone)
        if not os.path.exists(dir_cache):
            os.makedirs(dir_cache)

    def twale(self):
        self.logger.debug('twaler started')
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
        # TODO still have problem
        stmt = ('LOAD DATA LOCAL INFILE "seed.lst" INTO TABLE target_users '
                'FIELDS TERMINATED BY \"\\t\" LINES TERMINATED BY \"\\n\"')
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
            seeds = os.listdir(self.config['dir_seeds'])
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
                seeds = os.listdir(self.config['dir_seeds'])
            for seed in seeds:          # N.B. seed is a filename with seeds
                self.crawl(seed)

    def generateseeds(self):
        generator = generate_seeds.Generator(self.config, self.logger)
        generator.generate()

    def crawl(self, seed):
        '''Read seed file, download, process and load to database
        A time stamp universally identified the crawling instance'''
        timestamp = misc.timefunctions.datestamp()
        self.logger.info('Crawling file %s as %s' % (seed, timestamp))

        cache_dir = os.path.join(self.config['dir_cache'], timestamp)
        seedfile = os.path.join(self.config['dir_seeds'], seed)

        # crawl seedfile and save files into cache_dir
        crawler = crawl.Crawler(seedfile, cache_dir, self.config, self.logger)
        crawler.crawlloop()
        self.processAndLoad(timestamp)

        # Move seedfile out of seed directory
        cachepath = os.path.join(self.config['dir_cache'], timestamp,
                                 "seed["+seed+"].txt")
        seeddonepath = os.path.join(self.config['dir_seedsdone'], seed)
        shutil.copy(seedfile, cachepath)
        shutil.move(seedfile, seeddonepath)

    def processAndLoad(self, timestamp):
        # A folder under cache will be created with the timestamp
        cache_dir = os.path.join(self.config['dir_cache'], timestamp)
        processed_dir = os.path.join(cache_dir, "processed_crawl")
        # PROCESS
        self.logger.info("Processing instance " + timestamp)
        processor = process_crawl.Processor(self.config, self.logger,
                timestamp, cache_dir)
        processor.process_loop()
        # LOAD
        self.logger.info("Loading instance " + timestamp)
        loader = load_crawl.Loader(self.config, self.logger)
        loader.load(processed_dir)


if __name__ == '__main__':
    t = Twaler()
    t.twale()
