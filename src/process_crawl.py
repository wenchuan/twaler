#!/usr/local/bin/python3
import re
import os
import sys
import gzip
import xml.dom.minidom
from contextlib import closing

from config import parse_arguments
from config import timefunctions
from config import logger
from config import cache_accessor
from config import mysql_db
from config import file_db

"""Functions for XML DOM parsing"""
def gettext(node):
  children = node.childNodes
  rc = ""
  for child in children:
    if child.nodeType == child.TEXT_NODE:
      rc = rc + child.data
  return rc

def getChildByName(dom, name):
  for child in dom.childNodes:
    if child.nodeName == name:
      return child
  return None

class process_crawl():
  def __init__(self,instance,dir_log, dir_cache, dir_processed,process_userinfo, process_friends, process_tweets, process_listmembers, process_memberships, process_to_db, extract_mentions, extract_urls,extract_hashes,verbose,**kwargs):
    self.name = "process_crawl"
    if verbose:
      self.log = logger(self.name,dir_log).verbose_log
    else:
      self.log = logger(self.name,dir_log).log
    self.instanceTimeStamp = timefunctions.instanceToSqlTime(instance)
    self.dir_cache = dir_cache
    self.cache_accessor = cache_accessor(self.dir_cache,self.log)
    self.process_userinfo = process_userinfo
    self.process_friends = process_friends
    self.process_memberships = process_memberships
    self.process_tweets = process_tweets
    self.process_listmembers = process_listmembers
    self.extract_mentions = extract_mentions
    self.extract_urls = extract_urls
    self.extract_hashes = extract_hashes

    if process_to_db and kwargs["db_username"] and kwargs["db_password"] and kwargs["db_server"] and kwargs["db_database"]:
      self.db = mysql_db(kwargs["db_server"],kwargs["db_username"],kwargs["db_password"],kwargs["db_database"],self.log)
    else:
      if not os.path.exists(dir_processed):
        os.makedirs(dir_processed)
      self.db = file_db(dir_processed,self.log)

  """Parser Helpers"""

  def get_urls(self, tweet):
    urls = []
    for url in re.finditer(r"(http://[0-9a-zA-Z\$\-_\.\+\*\'\,:/@\?&;#=]+)([^0-9a-zA-Z\$\-_\.\+\*\'\,:/@\?&;#=]|$)", tweet, re.IGNORECASE):
      urls.append(url.group(1))
    return urls

  def get_retweets(self, tweet):
    rtusers = []
    for rt in re.finditer(r"RT @([a-zA-Z0-9_]+)", tweet, re.IGNORECASE):
      rtusers.append(rt.group(1))
    return rtusers

  def get_mentions(self,tweet):
    m_users = []
    for m in re.finditer(r"@([a-zA-Z0-9_]+)", tweet, re.IGNORECASE):
      m_users.append(m.group(1))
    return m_users

  def get_hashes(self,tweet):
    m_hashes = []
    for m in re.finditer(r"#([0-9]*[a-zA-Z]+[a-zA-Z0-9_]*)", tweet, re.IGNORECASE):
      m_hashes.append(m.group(1))
    return m_hashes

  """XML Parsers"""
  def store_userinfo(self,nid,file):
    self.log("processing " + file)
    #Open up the file for reading
    with closing(gzip.open(file, "r")) as fin:
      #read it into memory
      xmltxt = fin.read()
      #parse the rss
      xmldom = xml.dom.minidom.parseString(xmltxt)
      #Get the tweets
      users = xmldom.getElementsByTagName("user")
      for user in users:
        try:
          user_name = gettext(getChildByName(user,"name"))
          location = gettext(getChildByName(user,"location"))
          description = gettext(getChildByName(user,"description")).replace("\n"," ")
          url = gettext(getChildByName(user,"url"))
          followers_count = gettext(getChildByName(user,"followers_count"))
          friends_count = gettext(getChildByName(user,"friends_count"))
          status_count = gettext(getChildByName(user,"statuses_count"))
          raw_created_at = gettext(getChildByName(user,"created_at"))
          created_at = timefunctions.xmlToSqlTime(raw_created_at)
          #insert into db
          self.db.insert("users", (nid,user_name,location,description,url,followers_count,friends_count,status_count,created_at),{"user_name":user_name, "location": location, "description":description, "url":url, "followers_count": followers_count, "friends_count":friends_count, "status_count": status_count, "created_at":created_at})
        except Exception as e:
          self.log("Couldn't parse user info, exception:" + str(e))

  def store_tweets(self, nid, file):
    self.log("processing " + file)
    #Open up the file for reading
    with closing(gzip.open(file, "r")) as fin:
      #read it into memory
      rsstxt = fin.read()

      #parse the rss
      rssdom = xml.dom.minidom.parseString(rsstxt)
      #Get the tweets
      tweets = rssdom.getElementsByTagName("item")

      #get the first tweet for last_checked
      try:
        raw_last_tweet = gettext(getChildByName(tweets[0],"guid"))
        last_checked = re.search("/(\d*)$",raw_last_tweet).group(1)
        if not(last_checked):
          last_checked = "-1"
          self.log("Couldn't get first tweet, default to -1")
      except Exception as e:
          self.log("Couldn't get first tweet, exception:" + str(e))

      #Parse the XML
      for tweet in tweets:
        try:
          raw_tweet_id = gettext(getChildByName(tweet,"guid"))
          raw_date = gettext(getChildByName(tweet,"pubDate"))
          raw_text = gettext(getChildByName(tweet,"title")).replace("\n"," ")

          tweet_id = re.search("/(\d*)$",raw_tweet_id).group(1)
          user_name = re.search("twitter.com/(.*)/statuses",raw_tweet_id).group(1)
          text = re.search("%s: (.*)"%(user_name),raw_text).group(1)
          date = timefunctions.rssToSqlTime(raw_date)

          #insert into tweet database
          self.db.insert("tweets", (tweet_id,nid,date,text))

          #get mentions
          if (self.extract_mentions):
            rts = self.get_retweets(text)
            mts = self.get_mentions(text)
            for retweetee in rts:
              self.db.insert("mentions", (tweet_id,retweetee,"1"))
            for mentionee in mts:
              if mentionee in rts:
                continue
              self.db.insert("mentions", (tweet_id,mentionee,"0"))

          #get urls
          if (self.extract_urls):
            urls = self.get_urls(text)
            for url in urls:
              self.db.insert("urls", (tweet_id,url))

          #get hashes
          if(self.extract_hashes):
            hashes = self.get_hashes(text)
            for hash in hashes:
              self.db.insert("hash_tags",(tweet_id,hash))

        except Exception as e:
          self.log("Couldn't parse tweet, exception:" + str(e))

      return last_checked

  def store_friends(self, nid, file):
    self.log("processing " + file)
    try:
      #Open up the file for reading
      with closing(gzip.open(file, "r")) as fin:
        #read it into memory
        xmltxt = fin.read()
        #parse the xml
        xmldom = xml.dom.minidom.parseString(xmltxt)
        #Get the ids
        ids = xmldom.getElementsByTagName("id")
    #can't open file for reading...
    except Exception as e:
      self.log("Couldn't get friends\n" + str(e))

    for id in ids:
      fid = gettext(id)
      self.db.insert("friends", (str(nid),fid,self.instanceTimeStamp,self.instanceTimeStamp),{"date_last":self.instanceTimeStamp})

  def store_memberships(self, nid,file):
    self.log("processing " + file)
    try:
      #Open up the file for reading
      with closing(gzip.open(file, "r")) as fin:
        xmltxt = fin.read()
        xmldom = xml.dom.minidom.parseString(xmltxt)
        memberships = xmldom.getElementsByTagName("list")
      #can't open file for reading...
    except Exception as e:
      self.log("Couldn't get memberships\n" + str(e))

    for membership in memberships:
      try:
        #get list id
        list_id = gettext(getChildByName(membership, "id"))
        #get userid of list owner
        user = getChildByName(membership, "user")
        owner_id = gettext(getChildByName(user, "id"))
        list_name = gettext(getChildByName(membership, "full_name"))

        #create list entry if it doesn't already exist
        self.db.insert("lists", (list_id,list_name,owner_id),{"list_name":list_name,"list_owner":owner_id})

        #create list membership entry with user
        #update the last_seen if we've already seen this member
        self.db.insert("list_memberships", (list_id,str(nid),self.instanceTimeStamp,self.instanceTimeStamp), {"date_last":self.instanceTimeStamp})

      except Exception as e:
        self.log("Couldn't get list memberships %s" % e)
        continue

  def store_listmembers(self,nid,list_dir):
    lists = os.listdir(list_dir)
    #for all the lists under this user
    for list in lists:
      list_path = os.path.join(list_dir,list)
      infiles = os.listdir(list_path)
      for file in infiles:
        #If this is the type of file we are looking for
        if (file[:17] == 'members.xml.data.'):
          #Get full input filename with path
          fin_name = os.path.join(list_path,file)
          self.log("processing " + fin_name)
          try:
            #Open up the file for reading
            with closing(gzip.open(fin_name, "r")) as fin:
              xmltxt = fin.read()
              xmldom = xml.dom.minidom.parseString(xmltxt)
              members = xmldom.getElementsByTagName("user")
          #can't open file for reading...
          except Exception as e:
            self.log("Couldn't get members\n" + str(e))
            continue

          for member in members:
            try:
              member_id = gettext(getChildByName(member, "id"))
              #create list membership entry with user
              #update the last_seen if we've already seen this member
              self.db.insert("list_memberships", (list,member_id,self.instanceTimeStamp,self.instanceTimeStamp), {"last_updated":self.instanceTimeStamp})

            except Exception as e:
                self.log("Couldn't get list members %s" % e)
                continue
      self.db.insert("list_update", (list, self.instanceTimeStamp), {"list_updated":self.instanceTimeStamp})

  """Main Loop"""

  def process_loop(self):
    #Note the crawl instance we are processing
    self.db.insert("crawl_instances",(self.instanceTimeStamp,))
    #Go through every single id under the cache folder
    for ids in self.cache_accessor.idqueue():
      try:
        #keep the last updated instance for each user
        update_userinfo = ""
        update_tweets = ""
        update_friends = ""
        update_memberships = ""
        update_lastchecked = ""

        nid = ids[0]
        dir = ids[1]
        infiles = os.listdir(dir)

        #parse through each file under the user id folder
        for file in infiles:
          file_path = os.path.join(dir, file)
          if (self.process_userinfo and file[:18] == 'userinfo.xml.data.'):
            self.store_userinfo(nid, file_path)
            update_userinfo = self.instanceTimeStamp

          if (self.process_tweets and file[:16] == 'tweets.rss.data.'):
            update_lastchecked = self.store_tweets(nid, file_path)
            update_tweets = self.instanceTimeStamp

          if (self.process_friends and file[:17] == 'friends.xml.data.'):
            self.store_friends(nid, file_path)
            update_friends = self.instanceTimeStamp

          if (self.process_memberships and file[:21] == 'memberships.xml.data.'):
            self.store_memberships(nid, file_path)
            update_memberships = self.instanceTimeStamp

        self.db.insert("users_update", (nid, update_userinfo, update_tweets, update_friends, update_memberships, update_lastchecked), {"info_updated":update_userinfo,"tweet_updated":update_tweets,"friend_updated":update_friends,"membership_updated":update_memberships,"last_tweet_cursor":update_lastchecked})

        #check if the user has any lists under the folder
        if (self.process_listmembers):
          try:
            list_dir = os.path.join(dir,"lists")
            self.store_listmembers(nid, list_dir)
          except:
            continue
      except Exception as e:
        self.log("Couldn't parse user " + str(nid) + "\n\t" + str(e))
    self.db.__del__()
    self.log("process complete")


def usage():
  print("""\nUsage: %s [manual parameters] <config file>\n
Contents in configuration file:
*Values listed here are default values, used if parameter is unspecified
*Can also be overwritten with manual parameters
*ie) %s --dir_cache=cache config.txt

instance= <defaults to current time>\t[timestamp to signify crawl]
dir_log= log\t[disk location to write logs to]
dir_cache= cache\t[disk location of crawl results]
dir_processed= processed_crawl\t[directory to write processed tsv files to]
process_userinfo= 1
process_friends= 1
process_memberships= 1
process_tweets= 1
process_listmembers= 1
extract_mentions= 1
extract_urls= 1
extract_hashes=1
verbose = 1

[OPTIONAL: Directly process to DB]
process_to_db= 0\t[if 1, then directly insert into MySQL DB (must provide the db parameters)]
db_server= localhost\t[server for processing crawl results to MySQL DB]
db_database= twaler\t[database for processing crawl results to MySQL DB]
db_username= snorgadmin\t[MySQL DB username]
db_password= snorg321\t[MySQL DB password]
        """ % (sys.argv[0],sys.argv[0]))


if __name__ == '__main__':
  parameters = {"instance":timefunctions.datestamp(),"dir_log":"log","dir_cache":"cache","dir_processed":"processed_crawl","process_to_db":0,"db_server":"localhost","db_database":"twaler","db_username":"snorgadmin","db_password":"snorg321","process_userinfo":1,"process_friends":1,"process_memberships":1,"process_tweets":1,"process_listmembers":1,"extract_mentions":1,"extract_urls":1,"extract_hashes":1,"verbose":1}
  int_params = ["process_userinfo","process_friends","process_memberships",  "process_tweets","process_listmembers","extract_mentions","extract_urls","extract_hashes","verbose"]
  conf = parse_arguments(usage,parameters,int_params);
  crawler = process_crawl(**conf)
  crawler.process_loop()
