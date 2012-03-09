#!/usr/local/bin/python3
import re
import os
import sys
import gzip
import json
import xml.dom.minidom
from contextlib import closing

from misc import timefunctions
from misc import CacheAccessor
from misc import mysql_db
from misc import file_db

#-----XML DOM helper-------

def gettext(node):
    children = node.childNodes
    rc = ''
    for child in children:
        if child.nodeType == child.TEXT_NODE:
            rc = rc+child.data
    return rc

def getChildByName(dom, name):
    for child in dom.childNodes:
        if child.nodeName == name:
            return child
    return None


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

    # Parser helpers
    def get_urls(self, tweet):
        urls = []
        pat = (r"(http://[0-9a-zA-Z\$\-_\.\+\*\'\,:/@\?&;#=]+)"
                "([^0-9a-zA-Z\$\-_\.\+\*\'\,:/@\?&;#=]|$)")
        for url in re.finditer(pat, tweet, re.IGNORECASE):
            urls.append9url.group(1)
        return urls

    def get_retweets(self, tweet):
        rtusers = []
        return rtusers

    def get_mentions(self, tweet):
        return []

    def get_hashes(self, tweet):
        return []

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

    def store_friends(self, nid, filename):
        self.logger.debug('processing ' + filename)

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
                    # import pdb; pdb.set_trace()
                    filepath = os.path.join(dirpath, filename)
                    if filename.startswith('userinfo.json.data'):
                        self.store_userinfo(nid, filepath)
            except Exception as e:
                self.logger.error('Error during processing user %s : %s' %
                        (nid, e))
        self.db.__del__()
        self.logger.debug('processing complete')
