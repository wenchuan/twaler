#!/usr/local/bin/python3
from __future__ import with_statement

import os
import sys
import time
import urllib.request
import urllib.error
import http.client
import base64
import socket
import threading
import queue
import signal
import json

import misc

class Crawler:
    """Twitter Crawler Class
     Generates worker threads to crawl through the seeds directory

    """
    def __init__(self, seed_file, cache_dir, config, logger):
        self.seed_file = seed_file
        self.config = config
        self.logger = logger
        self.dir_cache = cache_dir
        if not os.path.exists(self.dir_cache):
            os.makedirs(self.dir_cache)
        self.idqueue = queue.Queue(0)

    def crawl(self):
        """Crawl and watch"""
        # two hackish functions to circumevent thread termination
        self.child = os.fork()
        if self.child == 0:
            self.crawlloop()    # child thread runs crawlloop
        else:
            self.watch()        # parent thread waits for interrupt signals

    def watch(self):
        """Wait for interrupt"""
        try:
            os.wait()
        except (KeyboardInterrupt, SystemExit):
            self.logger.warning("Keyboard Interrupt Received")
            os.kill(self.child, signal.SIGKILL)
        sys.exit()

    def crawlloop(self):
        """Main crawl loop"""
        self.workers = []       # set up workers
        for i in range(self.config['crawl_num_of_threads']):
            worker = _CrawlerWorker(self.idqueue, self.logger, self.dir_cache)
            worker.setName("Worker " + str(i))
            self.workers.append(worker)
            worker.start()
        try:
            # read seed file and crawl
            seedFileStream = open(os.path.join(self.seed_file),"r")
            self.logger.info("Crawling " + self.seed_file)
            while True:
                line = seedFileStream.readline()
                if not line:
                    break
                seed = line.strip()
                if seed[0] == "#":      # check for comments
                    break
                self.idqueue.put(seed)  # put seeds on queue
            self.idqueue.join()         # block until all seeds are processed
            self.logger.info("File " + self.seed_file + " completed")
            seedFileStream.close()
        except Exception as e:
            self.logger.error(str(e))
        for th in self.workers:         # send terminating signals to workers
            self.idqueue.put(_CrawlerWorker.TERMINATE_SIGNAL)
        for th in self.workers:         # wait for all workers to return
            th.join()
        self.logger.debug("Exited Successfully")


class _CrawlerWorker(threading.Thread):
    """ _CrawlerWorker represens a worker that crawl and save info"""
    workerLock = threading.Lock()
    CRAWL_USERINFO = "u"
    CRAWL_TWEETS = "t"
    CRAWL_FRIENDS = "f"
    TERMINATE_SIGNAL = "TERMINATE"

    def __init__(self, idqueue, logger, cachedir):
        threading.Thread.__init__(self)
        self.logger = logger
        self.cache_accessor = misc.CacheAccessor(cachedir, self.logger)
        # Twitter username and password is required for some requests
        self.username = "snorguser"
        self.password = "snorg321"
        self.pseudoseconds = 0              # XXX ???
        self.idqueue = idqueue

    def getrawheaders(self, page):
        """Dumps header content into a single string"""
        return "\r\n".join([(i + ": " + j) for i,j in page.getheaders()])

    def getconnection(self):
        """Return the cached HTTP connection"""
        try:
            if (self.twitter_connection):
                return self.twitter_connection
        except:
            pass
        self.twitter_connection = http.client.HTTPConnection("twitter.com:80")
        self.twitter_connection.connect()
        return self.twitter_connection

    def packauth(self, username, password):
        userpass = "%s:%s" % (username, password)
        base64string = base64.encodebytes(userpass.encode('utf8'))[:-1]
        auth = "Basic %s" % base64string.decode('utf8')
        return auth

    def gethttpresponse_auth(self, url, username=None,
                             password=None, datagzipped=False):
        """Get http response for url with authentication"""
        response = None
        for attempt in range(9):# try 9 times
            try:                # try to get a connection and do the request
                headers = {}
                headers["Connection"]="Keep-Alive"
                if (datagzipped):               # accept gzipped data
                    headers['Accept-encoding']='gzip'
                if (username and password):     # use auth info if given
                    auth = self.packauth(username,password)
                    headers["Authorization"] = auth
                connection = self.getconnection()
                connection.request("GET", url, headers=headers)
                response = connection.getresponse()
                if (response.code == 200):
                    return response
                elif (response.code == 302):
                    self.logger.error("302 received\n" +
                            self.getrawheaders(response))
                    raise Exception("redirection response")
                elif (response.code == 502):
                    self.logger.warning("502, connection error, "
                            "sleeping and retrying")
                    time.sleep(2)
                    self.twitter_connection = None
                # 400 is the code that twitter uses when your out of quota
                elif (response.code == 400):
                    while (True):
                        quota = self.getrequestquota()
                        if (quota > 0):
                            time.sleep(2)
                            break
                        self.logger.info("400, request quota: %d. Sleeping "
                                "10 more minutes and checking again" % quota)
                        time.sleep(10 * 60)
                else:
                    self.logger.warning("response code %d, sleeping and retrying"
                            % response.code)
                    time.sleep(2)
                    self.twitter_connection = None
            except http.client.NotConnected as e:
                self.logger.error(
                        "must have lost connection? resetting connection")
                self.twitter_connection = None
            except http.client.IncompleteRead as e:
                self.logger.error("incomplete read")
                time.sleep(10)
            except http.client.ImproperConnectionState as e:
                self.logger.error("Improper Connection State")
                time.sleep(10)
            except http.client.HTTPException as e:
                self.logger.error("HTTP Exception")
                time.sleep(10)
            except socket.error as e:
                self.logger.error("socket error: %s\nresetting connection"% e)
                self.twitter_connection = None
                time.sleep(5)
        return response

    def gethttpresponse(self, url, datagzipped=False):
        return self.gethttpresponse_auth(url, None, None, datagzipped)

    def getrequestquota(self, username=None, password=None):
        """Return the number of requests we have left"""
        URLRateLimit = "http://twitter.com/account/rate_limit_status.json"
        for attempt in range(10):   # make 10 attempts, stop if failed
            # Try and get rate limit
            try:
                req = urllib.request.Request(URLRateLimit)
                if (username and password):
                    req.add_header('Authorization',
                                   self.packauth(username, password))
                page = urllib.request.urlopen(req)
                if page.code == 200:
                    info = json.loads(page.read().decode())
                    return info['remaining_hits']
                #Request returned a bad code
                else:
                    self.logger.info("rate limit status request returned: %d" %
                            page.code);
            #Request caused an exception
            except urllib.error.HTTPError as e:
                self.logger.error("rate limit request failed. error %d"
                        % e.code)
            except urllib.error.URLError as e:
                self.logger.error("rate limit request failed. error %s" %
                        e.reason)
            self.logger.info("rate limit request failed: count:%d" % count)
            time.sleep(60)
        return 0

    def cache(self, request_type, uid, header, data,
              listname=None, datagzipped=False):
        self.cache_accessor.store_in_cache(request_type, uid, header, data,
                                           listname, datagzipped)
        return

    def fetch(self, seed):
        # check rate limit, sleep if no quota
        while (True):
            rq = self.getrequestquota()
            if (rq > 0):
                break
            self.logger.info(
                    "%s Request Quota: %d, sleeping for another 15 minutes" %
                    misc.timefunctions.datestamp(), rq)
            time.sleep(60 * 15)
        # parse the seed line
        seed = seed.split("\t")
        # case A: type unspecified on line, assumed to be just an user_id
        if len(seed) < 2:
            types = 'utf'                # the whole deal
            data = seed[0]
        # case B: type is specified
        else:
            types = seed[0]
            if '*' in types:
                types = 'utf'
            data = seed[1]
        # check for extra column separated by space
        fields = data.split(" ")
        uid = fields[0].strip()
        sid = None
        if len(fields) > 1:
            sid = fields[1].strip()
        # perform the fetch
        if _CrawlerWorker.CRAWL_USERINFO in types:
            self.fetch_userinfo(uid)
        if _CrawlerWorker.CRAWL_FRIENDS in types:
            self.fetch_friends(uid)
        if _CrawlerWorker.CRAWL_TWEETS in types:
            self.fetch_tweets(uid)

    def fetch_userinfo(self, uid):
        self.logger.debug("fetching userinfo for uid:%s " % uid)
        url = "http://api.twitter.com/1/users/show.json?user_id=" + uid
        page = self.gethttpresponse(url)
        if (page.code == 200):          # save headers and data on success
            headers = self.getrawheaders(page)
            data = page.read()
            self.cache("userinfo.json", uid, str(headers), data)
        else:                           # log problems if any
            self.logger.error('fail fetching userinfo for %s with '
                    'HTTP code %s' % (uid, page.code))
        return page.code

    def fetch_friends(self, uid):
        """reads first page of friends (people uid follows), tries 5 times
        if it fails with a 5xx error and finally reads next page if one is
        available returns 0 on succes, other number on error"""
        self.logger.debug("fetching friends for uid:%s" % uid)
        next_cursor = -1
        while(next_cursor != 0):      # while friend list is not complete
            url = ("http://api.twitter.com/1/friends/ids.json?"
                   "user_id=%s&cursor=%s" % (uid, next_cursor))
            page = self.gethttpresponse(url)
            if (page.code == 200):          # save header and data on success
                headers = self.getrawheaders(page)
                raw_data = page.read()
                data = json.loads(raw_data.decode())
                next_cursor = data['next_cursor']
                self.cache('friends.json', uid, str(headers), raw_data)
            else:
                self.logger.error('fail fetching friends for %s with '
                        'HTTP code %s' % (uid, page.code))
                return page.code
        return page.code

    def fetch_tweets(self, uid):
        self.logger.debug("fetching tweets for uid:%s" % uid)
        url = ("http://api.twitter.com/1/statuses/user_timeline.json?"
                "include_entities=t&trim_user=t&user_id=%s&count=200" % uid)
        page = self.gethttpresponse(url, True)
        if (page):
            datagzipped = ((page.headers["Content-Encoding"] == 'gzip'))
            import pdb; pdb.set_trace()
            if page.code == 200:        # save headers and data on success
                headers = self.getrawheaders(page)
                data = page.read()
                self.cache("tweets.json", uid, str(headers), data,
                           datagzipped=datagzipped)
        else:
            self.logger.error('fail fetching tweets for %s with '
                    'HTTP code %s' % (uid, page.code))
        return page.code

    def run(self):
        # get seed from queue and crawl until terminating signal encountered
        while True:
            try:
                seed = self.idqueue.get()       # blocking get
                self.logger.debug("thread gets: " + seed)
                if (seed == _CrawlerWorker.TERMINATE_SIGNAL):
                    self.logger.debug("termination signal received!")
                    self.idqueue.task_done()
                    break
                self.logger.debug("fetching \"%s\"" % seed)
                self.fetch(seed)
                self.idqueue.task_done()
            except Exception as e:
                self.logger.error(str(e))
                self.idqueue.task_done()
                break
        self.logger.debug("terminate signal received, closing thread")
