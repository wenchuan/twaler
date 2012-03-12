#!/usr/bin/python2.6
from __future__ import with_statement
import os
import sys
import time
import urllib2
import base64
import socket
import threading
import Queue
import signal
import json

import misc

class Crawler:
    """Twitter Crawler Class
     Generates worker threads to crawl through the seeds directory

    """
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.idqueue = Queue.Queue(0)

    def crawlloop(self, seed_file, cache_dir):
        seed_file = seed_file
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        workers = []       # set up workers
        for i in range(self.config['crawl_num_of_threads']):
            worker = _CrawlerWorker(self.idqueue, self.logger, cache_dir)
            worker.setName("Worker " + str(i))
            workers.append(worker)
            worker.start()
        try:
            # read seed file and crawl
            seedFileStream = open(os.path.join(seed_file),"r")
            self.logger.info("Crawling " + seed_file)
            while True:
                line = seedFileStream.readline()
                if not line:
                    break
                seed = line.strip()
                if seed[0] == "#":      # check for comments
                    break
                self.idqueue.put(seed)  # put seeds on queue
            self.idqueue.join()         # block until all seeds are processed
            self.logger.info("File " + seed_file + " completed")
            seedFileStream.close()
        except Exception as e:
            self.logger.error(str(e))
        for th in workers:         # send terminating signals to workers
            self.idqueue.put(_CrawlerWorker.TERMINATE_SIGNAL)
        for th in workers:         # wait for all workers to return
            th.join()
        self.logger.debug("crawl loop exit successfully")


class _CrawlerWorker(threading.Thread):
    """ _CrawlerWorker represens a worker that crawl and save info"""
    CRAWL_USERINFO = 'u'
    CRAWL_TWEETS = 't'
    CRAWL_FRIENDS = 'f'
    TERMINATE_SIGNAL = 'TERMINATE'

    def __init__(self, idqueue, logger, cachedir):
        threading.Thread.__init__(self)
        self.logger = logger
        self.cache_accessor = misc.CacheAccessor(cachedir, self.logger)
        self.idqueue = idqueue

    def gethttpresponse(self, url, datagzipped=False):
        """Get http response for url, ignore the header and return a
        pair consists of the content and whether the content is gzipped"""
        response = None
        request = urllib2.Request(url)
        if datagzipped:
            request.add_header('Accept-encoding', 'gzip')
        for attempt in xrange(9):
            try:
                response = urllib2.urlopen(request)
                headers = response.headers
                if response.code == 200:
                    gzipped = headers.getheader('content-encoding')
                    return (response.read(), gzipped)
                else:
                    self.logger.error('HTTP code %s on attempt _%s_ %s' %
                            (response.code, attempt, url))
            except urllib2.HTTPError as e:
                if e.code == 400:
                    self.logger.info("400, request quota: %d. Sleeping "
                                "10 more minutes and checking again" % quota)
                    time.sleep(10 * 60)
                else:
                    self.logger.error("HTTPError on attempt _%s_ %s %s" %
                            (attempt, url, e))
                    time.sleep(10)
            except urllib2.URLError as e:
                self.logger.error("URLError on attempt _%s_ %s %s: %s" %
                        (attempt, url, e.reason, e))
                time.sleep(10)
        return (None, None)

    def getrequestquota(self):
        """Return the number of requests we have left"""
        url = "http://twitter.com/account/rate_limit_status.json"
        for attempt in xrange(5):   # make 5 attempts, stop if failed
            # Try and get rate limit
            try:
                page = urllib2.urlopen(url)
                if page.code == 200:
                    info = json.loads(page.read().decode())
                    return info['remaining_hits']
                # Request returned a bad code
                self.logger.warning("rate limit status request returned: "
                        "%d %s" % (page.code, page.msg));
            # Request caused an exception
            except Exception as e:
                self.logger.error("rate limit request failed. error %s"
                        % str(e))
            self.logger.info("rate limit request failed")
            time.sleep(10)
        # no response for 5 times, return zero
        return 0

    def cache(self, request_type, uid, data, datagzipped=False):
        self.cache_accessor.store_in_cache(request_type, uid, data,
                                           datagzipped)
        return

    def fetch(self, seed):
        # check rate limit, sleep if no quota
        while (self.getrequestquota() == 0):
            self.logger.info('API limit reached, retry after 15 mins')
            time.sleep(15 * 60)
        # parse the seed line
        seed = seed.split()
        # Type unspecified on line, assumed to be user_id
        if len(seed) < 2:
            types = 'utf'
            uid = seed[0]
        else:
            types = seed[0]
            if '*' in types:
                types = 'utf'
            uid = seed[1]
        # download info based on type
        if _CrawlerWorker.CRAWL_USERINFO in types:
            self.fetch_userinfo(uid)
        if _CrawlerWorker.CRAWL_FRIENDS in types:
            self.fetch_friends(uid)
        if _CrawlerWorker.CRAWL_TWEETS in types:
            self.fetch_tweets(uid)

    def fetch_userinfo(self, uid):
        self.logger.debug("start fetching userinfo for uid:%s " % uid)
        url = "http://api.twitter.com/1/users/show.json?user_id=" + uid
        (page, gzipped) = self.gethttpresponse(url)
        self.cache("userinfo.json", uid, page, gzipped)
        self.logger.debug("fetched userinfo for uid:%s " % uid)

    def fetch_friends(self, uid):
        """reads first page of friends (people uid follows), tries 5 times
        if it fails with a 5xx error and finally reads next page if one is
        available returns 0 on succes, other number on error"""
        self.logger.debug("start fetching friends for uid:%s" % uid)
        next_cursor = -1
        while(next_cursor != 0):      # while friend list is not complete
            url = ("http://api.twitter.com/1/friends/ids.json?"
                   "user_id=%s&cursor=%s" % (uid, next_cursor))
            (page, gzipped) = self.gethttpresponse(url)
            data = json.loads(page.decode())
            next_cursor = data['next_cursor']
            self.cache('friends.json', uid, page, gzipped)
        self.logger.debug("fetched friends for uid:%s" % uid)

    def fetch_tweets(self, uid):
        self.logger.debug("start fetching tweets for uid:%s" % uid)
        url = ("http://api.twitter.com/1/statuses/user_timeline.json?"
                "include_entities=t&trim_user=t&user_id=%s&count=200" % uid)
        (page, gzipped) = self.gethttpresponse(url, True)
        self.cache("tweets.json", uid, page, gzipped)
        self.logger.debug("fetched tweets for uid:%s" % uid)

    def run(self):
        # get seed from queue and crawl until terminating signal encountered
        while True:
            try:
                seed = self.idqueue.get()       # blocking get
                self.logger.debug("thread gets: " + seed)
                if (seed == _CrawlerWorker.TERMINATE_SIGNAL):
                    self.idqueue.task_done()
                    break
                self.fetch(seed)
                self.idqueue.task_done()
            except Exception as e:
                self.logger.error(str(e))
                self.idqueue.task_done()
                break
        self.logger.debug("terminate signal received, closing thread")
