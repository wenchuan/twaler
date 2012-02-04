#!/usr/local/bin/python3
from __future__ import with_statement

import os
import sys
import time
import urllib.request
import urllib.error
import http.client
import base64
import xml.dom.minidom
import socket
import threading
import queue
import signal

from config import parse_arguments
from config import timefunctions
from config import logger
from config import cache_accessor

class crawler:
  """Twitter Crawler Class: Generates worker threads to crawl through the seeds directory"""

  def __init__(self, seed_file, dir_log, dir_cache, crawl_numOfThreads, verbose, **kwargs):
    if not os.path.exists(dir_cache):
      os.makedirs(dir_cache)

    self.name = "Crawler"
    self.seed_file = seed_file
    self.dir_cache = dir_cache
    self.logger = logger(self.name,dir_log) #create log object
    if verbose:
      self.log = self.logger.verbose_log
    else:
      self.log = self.logger.log
    self.verbose = verbose
    self.idqueue = queue.Queue(0)
    self.numOfThreads = crawl_numOfThreads
    #self.recovery_file = recovery_file

    self.logger.log("requests starting at %d" % crawlerWorker.getRequestQuota(self))

  """Crawl and Watch: Two hackish functions to circumvent python's multi-thread termination signal problem"""
  def crawl(self):
    #create a child thread
    self.child = os.fork()
    #make the child thread run crawloop
    if self.child == 0:
      self.crawloop()
    #make the parent thread wait for interrupt signals
    else:
      self.watch()

  def watch(self):
    try:
      os.wait()
    except (KeyboardInterrupt, SystemExit):
      #if keyboard interrupt is received
      self.log("Keyboard Interrupt Received")
      os.kill(self.child, signal.SIGKILL)
      #further recovery methods
    sys.exit()

  """Main crawl loop"""
  def crawloop(self):
    #set up crawl worker threads
    self.workers = []
    for i in range(self.numOfThreads):
      worker = crawlerWorker(self.idqueue, self.logger, self.dir_cache,self.verbose)
      worker.setName("Worker " + str(i))
      self.workers.append(worker)
      worker.start()

    #read seed file and crawl
    try:
      seedFileStream = open(os.path.join(self.seed_file),"r")
      self.log("Crawling " + self.seed_file)

      #read each line and put on queue for the worker threads
      while True:
        line = seedFileStream.readline()
        if not line:
          break
        seed = line.strip()
        self.idqueue.put(seed)

      #After all lines in the file are read, block until all items in the queue are completed
      self.idqueue.join()
      self.log("File " + self.seed_file + " completed")
      seedFileStream.close()

    except Exception as e:
      self.log(str(e))

    #send terminating signals
    for th in self.workers:
      self.idqueue.put(crawlerWorker.TERMINATE_SIGNAL)
    #wait for everyone to finish
    for th in self.workers:
      th.join()
    #Complete crawling!
    self.log("Exited Successfully")
    self.logger.__del__() #the logger isn't working as expected...

class crawlerWorker(threading.Thread):
  """ crawlerWorker: Worker thread that downloads and stores the crawled information"""
  workerLock = threading.Lock()

  CRAWL_USERINFO = "u"
  CRAWL_TWEETS = "t"
  CRAWL_FRIENDS = "f"
  CRAWL_LISTMEMBERSHIPS = "l"
  CRAWL_MEMBERS = "m"
  TERMINATE_SIGNAL = "TERMINATE"

  def __init__(self, idqueue, logger ,cachedir, verbose, **kwargs):
    threading.Thread.__init__(self)
    self.logger = logger
    if verbose:
      self.log = self.verboseLog
    else:
      self.log = self.silentLog
    self.cache_accessor = cache_accessor(cachedir,self.log)
    #Twitter username and password is required for some requests
    self.username = "snorguser"
    self.password = "snorg321"
    self.pseudoseconds = 0
    self.idqueue = idqueue

  """---Connection Methods---"""

  def getrawheaders(self,page):
    return "\r\n".join([(i + ": " + j) for i,j in page.getheaders()])

  def getconnection(self):
    try:
      if (self.twitter_connection):
        return self.twitter_connection
    except:
      pass
    self.twitter_connection = http.client.HTTPConnection("twitter.com:80")
    self.twitter_connection.connect()
    return self.twitter_connection

  def packauth(self, username,password):
    userpass = "%s:%s" % (username, password)
    base64string = base64.encodebytes(userpass.encode('utf8'))[:-1]
    auth = "Basic %s" % base64string.decode('utf8')
    return auth

  def gethttpresponse_auth(self, url, username=None, password=None, datagzipped=False):
    tries = 0
    response = None
    while (tries < 9):
      tries = tries + 1
      #try and get a connection and do the request
      try:
        headers={}
        #Always try and keep alive
        headers["Connection"]="Keep-Alive"
        #Accept gzipped data
        if (datagzipped):
          headers['Accept-encoding']='gzip'
        #If we're given a username and password, use them
        if (username and password):
          auth = self.packauth(username,password)
          headers["Authorization"] = auth
        connection = self.getconnection()
        connection.request("GET", url, headers=headers)
        response = connection.getresponse()
        #handle 302s (redirection)
        if (response.code == 200):
          return response
        elif (response.code == 302):
          print("302 received\n" + self.getrawheaders(response))
          raise Exception("redirection response")
        elif (response.code == 502):
          self.log("502, connection error, sleeping and retrying")
          time.sleep (2)
          self.twitter_connection = None
        #400 is the code that twitter uses when your out of quota
        elif (response.code == 400):
          while (True):
            quota = self.getRequestQuota()
            if (quota > 0):
              time.sleep(2)
              break
            self.log("400, request quota: %d. sleeping for 10 more minutes and checking again" % quota);
            time.sleep(10 * 60)
        else:
          self.log("response code %d, sleeping and retrying"%response.code)
          time.sleep (2)
          self.twitter_connection = None
      except http.client.NotConnected as e:
        self.log("must have lost connection? resetting connection")
        self.twitter_connection = None
      except http.client.IncompleteRead as e:
        self.log("incomplete read")
        time.sleep (10)
      except http.client.ImproperConnectionState as e:
        self.log("Improper Connection State")
        time.sleep (10)
      except http.client.HTTPException as e:
        self.log("HTTP Exception")
        time.sleep (10)
      except socket.error as e:
        self.log("socket error: %s\nresetting connection"% e)
        self.twitter_connection = None
        time.sleep (5)
    return response

  def gethttpresponse(self,url,datagzipped=False):
    tries = 0
    response = None
    while (tries < 9):
      tries = tries + 1
      #try and get a connection and do the request
      try:
        headers={}
        headers["Connection"]="Keep-Alive"
        if (datagzipped):
          headers['Accept-encoding']='gzip'
        connection = self.getconnection()
        connection.request("GET", url, headers=headers)
        response = connection.getresponse()
        if (response.code == 200):
          return response
        elif (response.code == 400):
          while (True):
            quota = self.getRequestQuota()
            if (quota > 0):
              time.sleep(2)
              break
            self.log("400, request quota: %d. sleeping for 10 more minutes and checking again" % quota);
            time.sleep(10 * 60)
        else:
          self.log("response code %d, sleeping and retrying"%response.code)
          time.sleep (2)
          self.twitter_connection = None
      except http.client.NotConnected:
        self.log("must have lost connection? resetting connection")
        self.twitter_connection = None
      except http.client.IncompleteRead:
        self.log("incomplete read")
        time.sleep (10)
      except http.client.ImproperConnectionState:
        self.log("Improper Connection State")
        time.sleep (10)
      except http.client.HTTPException:
        self.log("HTTP Exception")
        time.sleep (10)
      except socket.error:
        self.log("socket error, trying to sleep it off")
        time.sleep (7)
        self.twitter_connection = None
    return response

  def getRequestQuota(self, username=None, password=None):
    """Return the number of requests we have left"""
    url_rateLimit = "http://twitter.com/account/rate_limit_status.xml"
    count = 0
    #make 10 attempts, stop if failed
    while(count < 10):
      count = count + 1
      #Try and get rate limit
      try:
        req = urllib.request.Request(url_rateLimit)
        if (username and password):
          req.add_header('Authorization', self.packauth(username,password))
        page = urllib.request.urlopen(req)
        if page.code == 200:
          data = page.read()
          requota = str(data).split('<remaining-hits type=\"integer\">')[1].split("</remaining-hits>")[0]
          return int(requota)
        #Request returned a bad code
        else:
          self.log("rate limit status request returned: %d" % page.code);
      #Request caused an exception
      except urllib.error.HTTPError as e:
        self.log("rate limit request failed. with error %d" % (e.code))
      except urllib.error.URLError as e:
        self.log("rate limit request failed. with error %s" % (e.reason))
      self.log("something went wrong, sleeping for a minute: count:%d" % count)
      time.sleep(60)
    return(0)

  """---File IO Methods---"""

  def store_in_cache(self, request_type, uid, header, data, listname = None, datagzipped=False):
    self.cache_accessor.store_in_cache(request_type, uid, header, data, listname, datagzipped)

  """---Fetching Methods---"""

  def fetch(self, seed):
    #check rate limit, sleep if no quota
    while (True):
      rq = self.getRequestQuota()
      if (rq > 0):
        break
      self.log("%s Request Quota is %d, sleeping for another 15 minutes" % (timefunctions.datestamp(), rq))
      time.sleep(60 * 15)

    #parse the seed line
    seed = seed.split("\t")
    #For valid seed format, see Seedfile format in: http://wiki.cs.ucla.edu/twaler#crawl.py
    #case A: type unspecified on line, assumed to be just an user_id
    if len(seed) < 2:
      type = '*'
      data = seed[0]
    #case B: type is specified
    else:
      type = seed[0]
      data = seed[1]
    #check for extra column separated by space
    fields = data.split(" ")
    uid = fields[0].strip()
    sid = None
    #case C: extra column used to specify cursor or list_id
    if len(fields) > 1:
      sid = fields[1].strip()

    #perform the fetch
    for ch in type:
      if (ch == crawlerWorker.CRAWL_USERINFO):
        self.fetch_userinfo(uid)
      elif (ch == crawlerWorker.CRAWL_FRIENDS):
        self.fetch_friends(uid)
      elif (ch == crawlerWorker.CRAWL_TWEETS):
        self.fetch_tweets(uid, sid)
      elif (ch == crawlerWorker.CRAWL_LISTMEMBERSHIPS):
        self.fetch_listmemberships(uid)
      #crawling lists require userid, and listid
      elif (ch == crawlerWorker.CRAWL_MEMBERS):
        self.fetch_members(uid,sid)
      #match all
      elif (ch == "*"):
        uid = data.strip()
        self.fetch_userinfo(uid)
        self.fetch_friends(uid)
        self.fetch_tweets(uid, sid)
        self.fetch_listmemberships(uid)
      else:
        self.log("Type " +type+ " unrecognized for " + data)

  def fetch_userinfo(self,uid):
    """reads first page of friends (people uid follows), tries 5 times if it fails with a 5xx error
       and finally reads next page if one is available
       returns 0 on succes, other number on error"""
    self.log("fetching userinfo for uid:%s"%uid)
    url_userinfo = "http://api.twitter.com/1/users/show.xml?user_id=%s" % (uid)
    page = self.gethttpresponse(url_userinfo)
    #Success? save headers and data
    if (page.code == 200):
      headers = self.getrawheaders(page)
      data = page.read()
      #Get the next cursor
      self.store_in_cache("userinfo.xml", uid, str(headers), data)
      #Get updated request quota information
      self.reqsleft = int(page.getheader("X-RateLimit-Remaining"))
      #the problem with this is that clocks may be out of sync
      #rate_limit_reset = page.getheader("X-RateLimit-Reset")
      return page.code
    else:
      self.log("uid `%s` failed in fetch_userinfo. HTTP code: %s" % (uid, page.code))
      return page.code

  def fetch_friends(self,uid):
    """reads first page of friends (people uid follows), tries 5 times if it fails with a 5xx error
       and finally reads next page if one is available
       returns 0 on succes, other number on error"""
    self.log("fetching friends for uid:%s"%uid)
    #Get max id crawled
    next_cursor = -1
    #as long as we're not done (next_cursor != 0) and we haven't failed too much (tries < 5)
    while(next_cursor != '0'):
      #form URL
      url_tweets = "http://twitter.com/friends/ids.xml?user_id=%s&cursor=%s" % (uid,next_cursor)
      page = self.gethttpresponse(url_tweets)
      #Success? save headers and data
      if (page.code == 200):
        headers = self.getrawheaders(page)
        data = page.read()
        #Get the next cursor
        xmldom = xml.dom.minidom.parseString(data)
        cursor_node = xmldom.getElementsByTagName("next_cursor")[0]
        next_cursor = cursor_node.firstChild.data
        #Get the next cursor
        #next_cursor = str(data).split("\\n")[-2].split('<next_cursor>')[1].split('</next_cursor>')[0]
        self.store_in_cache("friends.xml", uid, str(headers), data)
        #Get updated request quota information
        self.reqsleft = int(page.getheader("X-RateLimit-Remaining"))
        #the problem with this is that clocks may be out of sync
        #rate_limit_reset = page.getheader("X-RateLimit-Reset")
        #We're done? return success
        if (next_cursor == '0'):
          return page.code
        #got this page but we still have more? continue to next URL
      else:
        self.log("uid `%s` failed in fetch_friends. HTTP code: %s" % (uid, page.code))
        return page.code
    #we should never get here
    return 666

  def fetch_listmemberships(self, uid):
    """get the lists this member belongs to"""
    self.log("fetching listmemberships for uid:%s"%uid)
    #Get max id crawled
    next_cursor = -1
    #We want to keep the files in the directory alphabetical order == chronological order
    self.pseudoseconds = 0
    #as long as we're not done (next_cursor != 0) and we haven't failed too much (tries < 5)
    while(next_cursor != '0'):
      #form URL
      url_tweets = "http://api.twitter.com/1/%s/lists/memberships.xml?cursor=%s" % (uid, next_cursor)
      #add authorization
      if ((not self.username) or (not self.password)):
        self.getuserpass()
      #Get the page
      page = self.gethttpresponse_auth(url_tweets, self.username, self.password)
      #Success? save headers and data
      if (page.code == 200):
        headers = self.getrawheaders(page)
        data = page.read()
        #Get the next cursor
        xmldom = xml.dom.minidom.parseString(data)
        cursor_node = xmldom.getElementsByTagName("next_cursor")[0]
        next_cursor = cursor_node.firstChild.data
        #next_cursor = str(data).split("\\n")[-2].split('<next_cursor>')[1].split('</next_cursor>')[0]
        self.store_in_cache("memberships.xml", uid, str(headers), data)
        #Get updated request quota information
        self.reqsleft = int(page.getheader("X-RateLimit-Remaining"))
        #the problem with this is that clocks may be out of sync
        #rate_limit_reset = page.getheader("X-RateLimit-Reset")
        #We're done? return success
        if (next_cursor == '0'):
          return page.code
        #got this page but we still have more? continue to next URL
      elif (page.code == 401):
        self.log("Authorization failed")
        self.getuserpass()
      else:
        self.log("uid `%s` failed in fetch_listmemberships. HTTP code: %s" % (uid, page.code))
        return page.code
    #Shouldn't really get here
    return 666

  def fetch_members(self, uid, listname):
    """get the lists members.
       The listid is the name he gave his list
       In the twitter world it would be @username/listid
       """
    self.log("fetching list members for uid:%s list:"%(uid,listname))
    if not(listname):
      self.log("uid `%s` failed in fetch_members. No listid" % (uid))
      return 666
    #Get max id crawled
    next_cursor = "-1"
    #We want to keep the files in the directory alphabetical order == chronological order
    self.pseudoseconds = 0
    #as long as we're not done (next_cursor != 0) and we haven't failed too much (tries < 5)
    while(next_cursor != '0'):
      #form URL
      url_tweets = "http://api.twitter.com/1/%s/%s/members.xml?cursor=%s" % (uid, listname, next_cursor)
      #add authorization
      if ((not self.username) or (not self.password)):
        self.getuserpass()
      #Get the page
      page = self.gethttpresponse_auth(url_tweets, self.username, self.password)
      #Success? save headers and data
      if (page.code == 200):
        headers = self.getrawheaders(page)
        data = page.read()
        #Get the next cursor
        xmldom = xml.dom.minidom.parseString(data)
        cursor_node = xmldom.getElementsByTagName("next_cursor")[0]
        next_cursor = cursor_node.firstChild.data
        #next_cursor = str(data).split("\\n")[-2].split('<next_cursor>')[1].split('</next_cursor>')[0]
        self.store_in_cache("members.xml", uid, str(headers), data, listname=listname)
        #Get updated request quota information
        self.reqsleft = int(page.getheader("X-RateLimit-Remaining"))
        #the problem with this is that clocks may be out of sync
        #rate_limit_reset = page.getheader("X-RateLimit-Reset")
        #We're done? return success
        if (next_cursor == '0'):
          return page.code
        #got this page but we still have more? continue to next URL
      elif (page.code == 401):
        self.log("Authorization failed")
        self.getuserpass()
      else:
        self.log("uid `%s` failed in fetch_members. HTTP code: %s" % (uid, page.code))
        return page.code
    #Shouldn't really get here
    return 666

  def fetch_tweets(self,uid, cursor):
    self.log("fetching tweets for uid:%s"%uid)
    #Get last cursor
    if cursor:
      last_crawled = cursor
    else:
      last_crawled = "-1"
    #form URL
    url_tweets = "http://twitter.com/statuses/user_timeline.rss?user_id=%s&since_id=%s&count=200" % (uid, last_crawled)
    page = self.gethttpresponse(url_tweets,True)
    if (page):
      #page = urllib.request.urlopen(url_tweets)
      datagzipped = ((page.headers["Content-Encoding"] == 'gzip'))
      #Success?
      if page.code == 200:
        #save headers and data
        headers = self.getrawheaders(page)
        data = page.read()
        self.store_in_cache("tweets.rss", uid, str(headers), data, datagzipped=datagzipped)

        #Get updated request quota information
        self.reqsleft = int(page.getheader("X-RateLimit-Remaining"))
        #the problem with this is that clocks may be out of sync
        #rate_limit_reset = page.getheader("X-RateLimit-Reset")

      else:
        self.log("uid `%s` failed in fetch_tweets. HTTP code: %s" % (uid, page.code))
      return page.code
    return 666

  """Log Functions for threads"""
  def silentLog(self, errmsg):
    crawlerWorker.workerLock.acquire()
    msg = "[" + self.name + "] %s: %s" % (timefunctions.datestamp(), errmsg)
    self.logger.errorlog.write(msg + "\n")
    crawlerWorker.workerLock.release()

  def verboseLog(self, errmsg):
    crawlerWorker.workerLock.acquire()
    msg = "[" + self.name + "] %s: %s" % (timefunctions.datestamp(), errmsg)
    print(msg)
    self.logger.errorlog.write(msg + "\n")
    crawlerWorker.workerLock.release()

  def run(self):
    #repeatedly, get seed from queue and crawl until terminating signal encountered
    while True:
      try:
        seed = self.idqueue.get() #blocking get

        if (seed == crawlerWorker.TERMINATE_SIGNAL):
          break
        #check for comments
        if seed[0] == "#":
          self.idqueue.task_done()
          continue

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
crawl_numOfThreads= 10\t[number of threads used for downloading]
verbose = 1
        """ % (sys.argv[0],sys.argv[0]))

if __name__ == '__main__':
  #the parameters and their default values
  parameters = {"seed_file":"seedfile.txt", "dir_log":"log", "dir_cache":"cache", "crawl_numOfThreads":10,"verbose":1}
  int_params = ["crawl_numOfThreads","verbose"]
  conf = parse_arguments(usage,parameters,int_params)
  crawler = crawler(**conf)
  crawler.crawl()