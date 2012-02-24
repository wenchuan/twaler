#!/usr/local/bin/python3
from __future__ import with_statement

import os
import sys
import time
import urllib.request
import urllib.error
import http.client
import xml.dom.minidom 
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
    def __init__(self, seed_file, dir_log,
                 dir_cache, crawl_num_of_threads, verbose, **kwargs):
        if not os.path.exists(dir_cache):
            os.makedirs(dir_cache)
        self.name = "Crawler"
        self.seed_file = seed_file
        self.dir_cache = dir_cache
        self.logger = misc.Logger(self.name, dir_log)
        if verbose:
            self.log = self.logger.verbose_log
        else:
            self.log = self.logger.log
        self.verbose = verbose
        self.idqueue = queue.Queue(0)
        self.num_of_threads = crawl_num_of_threads
        self.logger.log("requests starting at %d" %
                        _CrawlerWorker.getrequestquota(self))

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
            self.log("Keyboard Interrupt Received")
            os.kill(self.child, signal.SIGKILL)
        sys.exit()

    def crawlloop(self):
        """Main crawl loop"""
        self.workers = []       # set up workers
        for i in range(self.num_of_threads):
            worker = _CrawlerWorker(self.idqueue, self.logger,
                                   self.dir_cache, self.verbose)
            worker.setName("Worker " + str(i))
            self.workers.append(worker)
            worker.start()
        try:
            # read seed file and crawl
            seedFileStream = open(os.path.join(self.seed_file),"r")
            self.log("Crawling " + self.seed_file)
            while True:
                line = seedFileStream.readline()
                if not line:
                    break
                seed = line.strip()
                if seed[0] == "#":      # check for comments
                    break
                self.idqueue.put(seed)  # put seeds on queue
            self.idqueue.join()         # block until all seeds are processed
            self.log("File " + self.seed_file + " completed")
            seedFileStream.close()
        except Exception as e:
            self.log(str(e))
        for th in self.workers:         # send terminating signals to workers
            self.idqueue.put(_CrawlerWorker.TERMINATE_SIGNAL)
        for th in self.workers:         # wait for all workers to return
            th.join()
        self.log("Exited Successfully")
        self.logger.__del__() # XXX the logger isn't working as expected...


class _CrawlerWorker(threading.Thread):
    """ _CrawlerWorker represens a worker that crawl and save info"""
    workerLock = threading.Lock()
    CRAWL_USERINFO = "u"
    CRAWL_TWEETS = "t"
    CRAWL_FRIENDS = "f"
    CRAWL_LISTMEMBERSHIPS = "l"
    CRAWL_MEMBERS = "m"
    TERMINATE_SIGNAL = "TERMINATE"

    def __init__(self, idqueue, logger, cachedir, verbose, **kwargs):
        threading.Thread.__init__(self)
        if verbose:
            self.log = logger.verbose_log
        else:
            self.log = logger.silent_log
        self.cache_accessor = misc.CacheAccessor(cachedir, self.log)
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
                    print("302 received\n" + self.getrawheaders(response))
                    raise Exception("redirection response")
                elif (response.code == 502):
                    self.log("502, connection error, sleeping and retrying")
                    time.sleep(2)
                    self.twitter_connection = None
                # 400 is the code that twitter uses when your out of quota
                elif (response.code == 400):
                    while (True):
                        quota = self.getrequestquota()
                        if (quota > 0):
                            time.sleep(2)
                            break
                        self.log("400, request quota: %d. sleeping for 10 "
                                 "more minutes and checking again" % quota)
                        time.sleep(10 * 60)
                else:
                    self.log("response code %d, sleeping and retrying" %
                             response.code)
                    time.sleep(2)
                    self.twitter_connection = None
            except http.client.NotConnected as e:
                self.log("must have lost connection? resetting connection")
                self.twitter_connection = None
            except http.client.IncompleteRead as e:
                self.log("incomplete read")
                time.sleep(10)
            except http.client.ImproperConnectionState as e:
                self.log("Improper Connection State")
                time.sleep(10)
            except http.client.HTTPException as e:
                self.log("HTTP Exception")
                time.sleep(10)
            except socket.error as e:
                self.log("socket error: %s\nresetting connection"% e)
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
                    self.log("rate limit status request returned: %d" %
                             page.code);
            #Request caused an exception
            except urllib.error.HTTPError as e:
                self.log("rate limit request failed. error %d" % e.code)
            except urllib.error.URLError as e:
                self.log("rate limit request failed. error %s" % e.reason)
            self.log("rate limit request failed: count:%d" % count)
            time.sleep(60)
        return 0

    def cache(self, request_type, uid, header, data,
              listname=None, datagzipped=False):
        self.cache_accessor.store_in_cache(request_type, uid, header, data,
                                           listname, datagzipped)
        return

    def fetch(self, seed):
        print("try to fetch " + seed)
        # check rate limit, sleep if no quota
        while (True):
            rq = self.getrequestquota()
            if (rq > 0):
                break
            self.log("%s Request Quota: %d, sleeping for another 15 minutes" %
                     misc.timefunctions.datestamp(), rq)
            time.sleep(60 * 15)
        # parse the seed line
        seed = seed.split("\t")
        # case A: type unspecified on line, assumed to be just an user_id
        if len(seed) < 2:
            type = 'utf'                # the whole deal
            data = seed[0]
        # case B: type is specified
        else:
            type = seed[0]
            if '*' in type:
                type = 'utf'            # TODO: add more data type
            data = seed[1]
        # check for extra column separated by space
        fields = data.split(" ")
        uid = fields[0].strip()
        sid = None
        # case C: extra column used to specify cursor or list_id
        if len(fields) > 1:
            sid = fields[1].strip()
        # perform the fetch
        if _CrawlerWorker.CRAWL_USERINFO in type:
            self.fetch_userinfo(uid)
        if _CrawlerWorker.CRAWL_FRIENDS in type:
            self.fetch_friends(uid)
        if _CrawlerWorker.CRAWL_TWEETS in type:
            self.fetch_tweets(uid)
        if _CrawlerWorker.CRAWL_LISTMEMBERSHIPS in type:
            self.fetch_listmemberships(uid)
        if _CrawlerWorker.CRAWL_MEMBERS in type:
            self.fetch_members(uid)

    def fetch_userinfo(self, uid):
        self.log("fetching userinfo for uid:%s " % uid)
        url = "http://api.twitter.com/1/users/show.xml?user_id=" + uid
        page = self.gethttpresponse(url)
        if (page.code == 200):          # save headers and data on success
            headers = self.getrawheaders(page)
            data = page.read()
            self.cache("userinfo.xml", uid, str(headers), data)
            self.reqsleft = int(page.getheader("X-RateLimit-Remaining"))
            # XXX the problem with this is that clocks may be out of sync
            # rate_limit_reset = page.getheader("X-RateLimit-Reset")
            return page.code
        else:                           # log problems if any
            self.log("uid `%s` failed in fetch_userinfo. HTTP code: %s" %
                     (uid, page.code))
            return page.code

    def fetch_friends(self, uid):
        """reads first page of friends (people uid follows), tries 5 times
        if it fails with a 5xx error and finally reads next page if one is
        available returns 0 on succes, other number on error"""
        self.log("fetching friends for uid:%s" % uid)
        next_cursor = -1                # crawl the entire friend list
        while(next_cursor != '0'):      # while friend list is not complete
            url = ("http://api.twitter.com/1/friends/ids.xml?"
                   "user_id=%s&cursor=%s" % (uid, next_cursor))
            page = self.gethttpresponse(url)
            if (page.code == 200):          # save header and data on success
                headers = self.getrawheaders(page)
                data = page.read()
                # get the next cursor
                # TODO: change XML to JSON
                xmldom = xml.dom.minidom.parseString(data)
                cursor_node = xmldom.getElementsByTagName("next_cursor")[0]
                next_cursor = cursor_node.firstChild.data
                self.cache("friends.xml", uid, str(headers), data)
                # update request quota information
                self.reqsleft = int(page.getheader("X-RateLimit-Remaining"))
                # we're done? return success
                if (next_cursor == '0'):
                    return page.code
                # got this page but we still have more? continue to next URL
            else:
                self.log("uid `%s` failed in fetch_friends. HTTP code: %s" %
                         (uid, page.code))
                return page.code
        # we should never get here
        return 666

    def fetch_tweets(self, uid, since_id=None):
        self.log("fetching tweets for uid:%s" % uid)
        # Get last since_id
        if not since_id:
            since_id = "-1"
        # XXX ??? rss ???
        url = ("http://twitter.com/statuses/user_timeline.rss?"
               "user_id=%s&since_id=%s&count=200" % (uid, since_id))
        page = self.gethttpresponse(url, True)
        if (page):
            datagzipped = ((page.headers["Content-Encoding"] == 'gzip'))
            if page.code == 200:        # save headers and data on success
                headers = self.getrawheaders(page)
                data = page.read()
                self.cache("tweets.rss", uid, str(headers), data,
                           datagzipped=datagzipped)
            # update request quota information
            self.reqsleft = int(page.getheader("X-RateLimit-Remaining"))
        else:
            self.log("uid `%s` failed in fetch_tweets. HTTP code: %s" %
                     (uid, page.code))
            return page.code
        return 666

    def run(self):
        # get seed from queue and crawl until terminating signal encountered
        while True:
            try:
                seed = self.idqueue.get()       # blocking get
                print("thread gets: " + seed)
                if (seed == _CrawlerWorker.TERMINATE_SIGNAL):
                    print("termination signal received!")
                    self.idqueue.task_done()
                    break
                print("thread gets: " + seed)
                self.log("fetching \"%s\"" % seed)
                self.fetch(seed)
                self.idqueue.task_done()
            except Exception as e:
                self.log(str(e))
                self.idqueue.task_done()
                break
        self.log("terminate signal received, closing thread")

def usage():
    print("""\nUsage: %s [manual parameters] <config file>\n
            Contents in configuration file:
*Values listed here are default values, used if parameter is unspecified
*Can also be overwritten with parameters
*ie) %s --seed_file=seedfile.txt config.txt

seed_file=seedfile.txt\t[seed file]
dir_log= log\t[disk location to write logs to]
dir_cache= cache\t[disk location of crawl results]
crawl_num_of_threads= 10\t[number of threads used for downloading]
verbose = 1
        """ % (sys.argv[0],sys.argv[0]))

def main(argv=None):
    crawler = Crawler("seedfile.txt", "log", "cache", 4, 1)
    crawler.crawl()

if __name__ == '__main__':
    #the parameters and their default values
    parameters = {"seed_file":"seedfile.txt", "dir_log":"log", "dir_cache":"cache", "crawl_num_of_threads":10,"verbose":1}
    int_params = ["crawl_num_of_threads","verbose"]
    conf = misc.parse_arguments(usage, parameters, int_params)
    crawler = Crawler(**conf)
    crawler.crawl()
