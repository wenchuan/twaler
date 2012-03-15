#!/usr/bin/python2.6
import os
import shutil
import sys
import signal
import json
import logging
import logging.handlers

import crawler
import processor
import loader
import generator
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

        self.generator = generator.Generator(self.config, self.logger)
        self.crawler = crawler.Crawler(self.config, self.logger)
        self.processor = processor.Processor(self.config, self.logger)
        self.loader = loader.Loader(self.config, self.logger)

    def twale(self):
        self.logger.debug('twaler started')
        self.child = os.fork()              # create a child process
        if self.child == 0:
            self.twalerloop()               # child works, father watches
            #self.watchlist()
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
                self.logger.info("Start to generate new seeds")
                self.generator.generate()
                self.logger.info('Generate seeds complete')
                seeds = os.listdir(self.config['dir_seeds'])
            for seed in seeds:  # N.B. seed is a filename with seeds
                self.crawl(seed)

    def crawl(self, seed):
        '''Read seed file, download, process and load to database
        A time stamp universally identified the crawling instance'''
        timestamp = misc.timefunctions.datestamp()
        self.logger.info('Crawling file %s as %s' % (seed, timestamp))

        cache_dir = os.path.join(self.config['dir_cache'], timestamp)
        seedfile = os.path.join(self.config['dir_seeds'], seed)
        processed_dir = os.path.join(cache_dir, "processed_crawl")

        # Crawl
        self.crawler.crawl(seedfile, cache_dir)

        # Process
        self.logger.info("Processing instance " + timestamp)
        self.processor.process(timestamp, cache_dir)
        self.logger.info("Processing instance %s COMPLETE" % timestamp)

        # Load
        self.logger.info("Loading instance " + timestamp)
        self.loader.load(processed_dir)
        self.logger.info("Loading instance %s COMPLETE" % timestamp)

        # Move seedfile out of seed directory
        cachepath = os.path.join(self.config['dir_cache'], timestamp,
                                 "seed["+seed+"].txt")
        seeddonepath = os.path.join(self.config['dir_seedsdone'], seed)
        shutil.copy(seedfile, cachepath)
        shutil.move(seedfile, seeddonepath)


if __name__ == '__main__':
    t = Twaler()
    t.twale()
