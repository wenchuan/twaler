#!/usr/local/bin/python3
import re
import os
import sys
import gzip
import json
from contextlib import closing

from misc import timefunctions
from misc import CacheAccessor
from misc import mysql_db
from misc import file_db

class Processor():
    def __init__(self, config, logger, instance, cache_dir):
        self.config = config
        self.logger = logger
        self.dir_cache = cache_dir

        self.instance = timefunctions.instanceToSqlTime(instance)
        self.cache_accessor = CacheAccessor(self.dir_cache, self.logger)

        dir_processed = os.path.join(cache_dir, 'processed_crawl')
        if not os.path.exists(dir_processed):
            os.makedirs(dir_processed)
        self.db = file_db(dir_processed, self.logger)

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
                    timefunctions.jsonToSqlTime(data['created_at'])))
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
                        timefunctions.jsonToSqlTime(t['created_at']),
                        t['text']))
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

    def process_loop(self):
        self.db.insert('crawl_instances', (self.instance,))
        # Go through every single id under the cache folder
        for ids in self.cache_accessor.idqueue():
            try:
                update_userinfo = ''
                update_tweets = ''

                nid = ids[0]
                dirpath = ids[1]
                files = os.listdir(dirpath)

                for filename in files:
                    filepath = os.path.join(dirpath, filename)
                    if filename.startswith('userinfo.json.data'):
                        self.store_userinfo(nid, filepath)
                    if filename.startswith('friends.json.data'):
                        self.store_friends(nid, filepath)
                    if filename.startswith('tweets.json.data'):
                        self.store_tweets(nid, filepath)
            except Exception as e:
                self.logger.error('Error during processing user %s : %s' %
                        (nid, e))
        self.db.__del__()
        self.logger.debug('processing complete')
