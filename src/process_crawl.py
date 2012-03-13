#!/usr/bin/python2.6
import re
import os
import sys
import gzip
import json
import logging
from contextlib import closing

import misc

class Processor():
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    def process(self, instance, cache_dir):
        self.instance = misc.timefunctions.instanceToSqlTime(instance)
        self.cache_accessor = misc.CacheAccessor(cache_dir, self.logger)
        dir_processed = os.path.join(cache_dir, 'processed_crawl')
        if not os.path.exists(dir_processed):
            os.makedirs(dir_processed)
        self.db = misc.file_db(dir_processed, self.logger)
        # Go through every single id under the cache folder
        for ids in self.cache_accessor.idqueue():
            try:
                update_userinfo = ''
                update_friends = ''
                update_tweets = ''

                nid = ids[0]
                dirpath = ids[1]
                files = os.listdir(dirpath)

                for filename in files:
                    filepath = os.path.join(dirpath, filename)
                    if filename.startswith('userinfo.json.data'):
                        self.store_userinfo(nid, filepath)
                        update_userinfo = self.instance
                    if filename.startswith('friends.json.data'):
                        self.store_friends(nid, filepath)
                        update_friends = self.instance
                    if filename.startswith('tweets.json.data'):
                        self.store_tweets(nid, filepath)
                        update_tweets = self.instance
                self.db.insert('users_update', (
                    nid,
                    update_userinfo,
                    update_tweets,
                    update_friends))
            except Exception as e:
                self.logger.error('Error during processing user %s : %s' %
                        (nid, e))
        self.db.__del__()
        self.logger.debug('processing complete')

    def store_userinfo(self, nid, filename):
        '''put them in cache'''
        self.logger.debug('processing ' + filename)
        # Open up downloaded file for reading
        with closing(gzip.open(filename, 'r')) as fin:
            try:
                data = json.loads(fin.read().decode())
                url = data['url']
                if not url:
                    url = ''
                self.db.insert('users', (
                    nid,
                    data['name'],
                    data['screen_name'],
                    data['location'],
                    data['description'],
                    url,
                    data['followers_count'],
                    data['friends_count'],
                    data['statuses_count'],
                    misc.timefunctions.jsonToSqlTime(data['created_at'])))
            except Exception as e:
                self.logger.error("Can't parse userinfo, error: " + str(e))

    def store_tweets(self, nid, filename):
        self.logger.debug('processing ' + filename)
        # Open up downloaded file for reading
        with closing(gzip.open(filename, 'r')) as fin:
            try:
                data = json.loads(fin.read().decode())
                for t in data:
                    tid = t['id']
                    self.db.insert('tweets', (
                        tid,
                        nid,
                        t['retweet_count'],
                        misc.timefunctions.jsonToSqlTime(t['created_at']),
                        t['text']))
                    for h in t['entities']['hashtags']:
                        self.db.insert('hashtags', (
                            tid,
                            h['text']))
                    for u in t['entities']['urls']:
                        self.db.insert('urls', (
                            tid,
                            u['url']))
                    for m in t['entities']['user_mentions']:
                        self.db.insert('mentions', (
                            tid,
                            m['id']))
            except Exception as e:
                self.logger.error("Can't parse userinfo, error: " + str(e))

    def store_friends(self, nid, filename):
        self.logger.debug('processing ' + filename)
        # Open up downloaded file for reading
        with closing(gzip.open(filename, 'r')) as fin:
            try:
                data = json.loads(fin.read().decode())
                ids = data['ids']
                for fid in ids:
                    self.db.insert('friends', (
                        nid,
                        fid,
                        self.instance,
                        self.instance))
            except Exception as e:
                self.logger.error("Can't parse userinfo, error: " + str(e))


def main():
    # Get cache path from parameter
    if len(sys.argv) < 2:
        print('Usage:%s cache/2012.03.10.10.10.10' % sys.argv[0])
        sys.exit(0)
    if not os.path.exists(sys.argv[1]):
        print('Usage:%s cache/2012.03.10.10.10.10' % sys.argv[0])
        sys.exit(0)
    cache_dir = sys.argv[1]

    # Load global configurations
    fp = open('config.json')
    config = json.load(fp)
    fp.close()

    # Setup logger
    formatter = logging.Formatter(
            '%(asctime)-6s: %(funcName)s(%(filename)s:%(lineno)d) - '
            '%(levelname)s - %(message)s')
    consoleLogger = logging.StreamHandler()
    consoleLogger.setLevel(logging.DEBUG)
    consoleLogger.setFormatter(formatter)
    logging.getLogger('').addHandler(consoleLogger)
    logger = logging.getLogger('')
    logger.setLevel(logging.DEBUG)

    # Instantiate a processor
    processor = Processor(config, logger)
    processor.process(misc.timefunctions.datestamp(), cache_dir)

if __name__ == "__main__":
    main()
