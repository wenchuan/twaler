#!/usr/local/bin/python3
import sys
import os

from misc import parse_arguments
from misc import Logger


class Load_crawl():
  def __init__(self,instance, dir_log, dir_processed,db_username, db_password, db_database, db_server,verbose,**kwargs):
    self.name = "dbloader"
    if verbose:
      self.log = Logger(self.name,dir_log).verbose_log
    else:
      self.log = Logger(self.name,dir_log).log
    self.dir_data = dir_processed
    self.instance = instance
    if not(db_username) or not(db_password) or not(db_database) or not(db_server):
      usage()
      sys.exit(2)
    self.db_username = db_username
    self.db_password = db_password
    self.db_database = db_database
    self.db_server = db_server

  """Bulk Load a File"""
  def load(self,table,file,replace=True):
    if replace:
      replace_str = "REPLACE"
    else:
      replace_str = "IGNORE"
    stmt = "LOAD DATA LOCAL INFILE \"%s\" %s INTO TABLE %s FIELDS TERMINATED BY \"\\t\" LINES TERMINATED BY \"\\n\""%(file,replace_str,table)
    self.execute(stmt)

  """Bulk Load + Update by loading to a temporary table first"""
  def load_update(self,table,file, update_keys):
    #clear out temp table
    stmt = "DELETE FROM temp."+table
    self.execute(stmt)

    #bulk load to table
    stmt = "LOAD DATA LOCAL INFILE \"%s\" INTO TABLE temp.%s FIELDS TERMINATED BY \"\\t\" LINES TERMINATED BY \"\\n\""%(file,table)
    self.execute(stmt)

    #insert/update to official table
    stmt = "INSERT INTO %s SELECT * FROM temp.%s ON DUPLICATE KEY UPDATE "%(table,table)
    for key in update_keys:
      stmt += "%s= temp.%s.%s,"%(key,table,key)
    stmt = stmt[:-1]
    self.execute(stmt)
    #delete temp table
    stmt = "DELETE FROM temp."+table
    self.execute(stmt)
    #return number of rows

  def execute(self,stmt):
    mysqlstmt = "mysql -u %s -p%s %s -e '"%(self.db_username, self.db_password,self.db_database) + stmt + "'"
    os.system(mysqlstmt)
    self.log("executed:" + mysqlstmt)

  """Main Loop"""

  def load_loop(self):
    files = os.listdir(self.dir_data)

    #load each file that corresponds to the table name in the directory
    for file in files:
      filepath = os.path.join(self.dir_data,file)
      if (file == 'users.tsv'):
        self.load("users",filepath)
      if (file == 'tweets.tsv'):
        self.load("tweets",filepath)
      if (file == 'mentions.tsv'):
        self.load("mentions",filepath)
      if (file == 'urls.tsv'):
        self.load("urls",filepath)
      if (file == 'hashes.tsv'):
        self.load("hashes",filepath)
      if (file == 'crawl_instances.tsv'):
        self.load("crawl_instances",filepath)
      if (file == 'hash_tags.tsv'):
        self.load("hash_tags",filepath)
      if (file == 'friends.tsv'):
        self.load_update("friends",filepath,["date_last"])
      if (file == 'users_update.tsv'):
        self.load_update("users_update",filepath,["info_updated","tweet_updated","friend_updated","membership_updated","last_tweet_cursor"])

def usage():
  print("""\nUsage: %s [manual parameters] <config file>\n
Contents in configuration file:
*Values listed here are default values, used if parameter is unspecified
*Can also be overwritten with manual parameters
*ie) %s --dir_cache=cache config.txt

dir_log= log\t[disk location to write logs to]
dir_processed= processed_crawl\t[directory to write processed tsv files to]
db_server= localhost\t[server for processing crawl results to MySQL DB]
db_database= twaler\t[database for processing crawl results to MySQL DB]
db_username= snorgadmin\t[MySQL DB username]
db_password= snorg321\t[MySQL DB password]
        """ % (sys.argv[0],sys.argv[0]))


if __name__ == '__main__':
  parameters = {"dir_log":"log","dir_processed":"processed_crawl","db_server":"localhost","db_database":"twaler","db_username":"snorgadmin","db_password":"snorg321"}
  conf = parse_arguments(usage,parameters,[]);

  loader = Load_crawl(**conf)
  loader.load_loop()
