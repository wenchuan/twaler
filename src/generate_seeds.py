#!/usr/local/bin/python3

import os
import sys

from config import parse_arguments
from config import timefunctions
from config import mysql_db
from config import logger

class generate_seeds():
  def __init__(self,instance,dir_log,dir_seeds,db_username, db_password, db_database, db_server, seed_userinfo,seed_tweets,seed_friends,seed_listmemberships,seed_lists,seed_per_file,seed_limit,update_limit,list_limit,verbose,**kwargs):
    try:
      self.name = "generate_seeds"
      self.dir_seedout = dir_seeds
      if not os.path.exists(dir_seeds):
        os.makedirs(dir_seeds)
      if verbose:
        self.log = logger(self.name,dir_log).verbose_log
      else:
        self.log = logger(self.name,dir_log).log
      self.instance = instance
      self.instanceTimeStamp = timefunctions.instanceToSqlTime(instance)
      self.mysql_db = mysql_db(db_server,db_username,db_password,db_database,self.log)
      self.seed_per_file = seed_per_file
      self.seed_limit = seed_limit
      self.list_limit = list_limit

      self.seed_userinfo = seed_userinfo
      self.seed_tweets= seed_tweets
      self.seed_friends= seed_friends
      self.seed_listmemberships= seed_listmemberships
      self.seed_lists= seed_lists

    except Exception as e:
      self.log(str(e))

  def generate(self):
    if (self.seed_userinfo or self.seed_tweets or self.seed_friends or self.seed_listmemberships):
      self.generate_newUsers()
      self.generate_oldUsers()
    if (self.seed_lists):
      self.generate_lists()


  def generate_newUsers(self):
    try:
      #Method: select the most re-occuring friend that has yet to be crawled
      getFrenStmt = "SELECT friend_id FROM friends WHERE friend_id NOT IN (SELECT user_id FROM users_update) group by friend_id order by count(*) desc limit " + str(self.seed_limit)
      self.mysql_db.execute(getFrenStmt)
      self.log("MySQL generate_users Query Complete")
      #seed type
      seedType = ''
      if self.seed_userinfo and self.seed_tweets and self.seed_friends and self.seed_listmemberships:
        seedType = '*'
      else:
        if self.seed_userinfo:
          seedType += 'u'
        if self.seed_tweets:
          seedType += 't'
        if self.seed_friends:
          seedType += 'f'
        if self.seed_listmemberships:
          seedType += 'l'

      #fetch results and write to seed file
      newFrens = self.mysql_db.cursor.fetchall()
      currSeedFile = 0
      currSeed = 0
      seedFileOut = open(os.path.join(self.dir_seedout,"seeds_"+self.instance+"_"+str(currSeedFile)+".txt"),"w")

      for user in newFrens:
        #open new file when seed per file limit is reached
        if currSeed == self.seed_per_file:
          self.log("generate_user file " + "seeds_"+self.instance+"_"+str(currSeedFile)+".txt Completed")
          seedFileOut.close()
          currSeedFile += 1
          seedFileOut = open(os.path.join(self.dir_seedout,"seeds_"+self.instance+"_"+str(currSeedFile)+".txt"),"w")
          currSeed = 0

        seedFileOut.write(seedType + "\t" + str(user[0])+"\n")
        currSeed += 1

      self.log("generate_user file " + "seeds_"+self.instance+"_"+str(currSeedFile)+".txt Completed")
      seedFileOut.close()

    except Exception as e:
      self.log(str(e))

  def generate_lists(self):
    try:
      #Method: generate lists
      getListStmt = "SELECT list_id, list_owner FROM lists WHERE list_id NOT IN (SELECT u.list_id FROM list_update u) group by list_id order by count(*) desc limit " + str(self.list_limit)
      self.mysql_db.execute(getListStmt)
      self.log("MySQL generate_lists Query Complete")

      #fetch results and write to seed file
      newLists = self.mysql_db.cursor.fetchall()
      currSeedFile = 0
      currSeed = 0
      seedFileOut = open(os.path.join(self.dir_seedout,"listseeds_"+self.instance+"_"+str(currSeedFile)+".txt"),"w")

      for list in newLists:
        #open new file when seed per file limit is reached
        if currSeed == self.seed_per_file:
          self.log("generate_lists file " + "listseeds_"+self.instance+"_"+str(currSeedFile)+".txt Completed")
          seedFileOut.close()
          currSeedFile += 1
          seedFileOut = open(os.path.join(self.dir_seedout,"listseeds_"+self.instance+"_"+str(currSeedFile)+".txt"),"w")
          currSeed = 0

        seedFileOut.write("m\t" + str(list[1])+ " " + str(list[0])+"\n")
        currSeed += 1

      self.log("generate_lists file " + "listseeds_"+self.instance+"_"+str(currSeedFile)+".txt Completed")
      seedFileOut.close()

    except Exception as e:
        self.log(str(e))

  def generate_oldUsers(self):
    try:
      #Method: select the most re-occuring friend that has yet to be crawled
      getFrenStmt = "SELECT user_id FROM users_update Order by info_updated desc limit " + str(self.seed_limit)
      self.mysql_db.execute(getFrenStmt)
      self.log("MySQL generate_users Query Complete")
      #seed type
      seedType = ''
      if self.seed_userinfo and self.seed_tweets and self.seed_friends and self.seed_listmemberships:
        seedType = '*'
      else:
        if self.seed_userinfo:
          seedType += 'u'
        if self.seed_tweets:
          seedType += 't'
        if self.seed_friends:
          seedType += 'f'
        if self.seed_listmemberships:
          seedType += 'l'

      #fetch results and write to seed file
      oldUsers = self.mysql_db.cursor.fetchall()
      currSeedFile = 0
      currSeed = 0
      seedFileOut = open(os.path.join(self.dir_seedout,"update_"+self.instance+"_"+str(currSeedFile)+".txt"),"w")

      for user in oldUsers:
        #open new file when seed per file limit is reached
        if currSeed == self.seed_per_file:
          self.log("generate_user file " + "update_"+self.instance+"_"+str(currSeedFile)+".txt Completed")
          seedFileOut.close()
          currSeedFile += 1
          seedFileOut = open(os.path.join(self.dir_seedout,"update_"+self.instance+"_"+str(currSeedFile)+".txt"),"w")
          currSeed = 0

        seedFileOut.write(seedType + "\t" + str(user[0])+"\n")
        currSeed += 1

      self.log("generate_user file " + "update_"+self.instance+"_"+str(currSeedFile)+".txt Completed")
      seedFileOut.close()

    except Exception as e:
      self.log(str(e))
def usage():
  print("""\nUsage: %s [manual parameters] <config file>\n
Contents in configuration file:
*Values listed here are default values, used if parameter is unspecified
*Can also be overwritten with parameters
*ie) %s --seed_file=seedfile.txt config.txt

instance= <defaults to current time>
dir_log= log\t[disk location to write logs to]
dir_seeds = seeds \t[disk location to write the seed files to]
db_server= localhost\t[server for processing crawl results to MySQL DB]
db_database= twaler\t[database for processing crawl results to MySQL DB]
db_username= snorgadmin\t[MySQL DB username]
db_password= snorg321\t[MySQL DB password]
seed_userinfo = 1
seed_tweets= 1
seed_friends= 1
seed_listmemberships= 1
seed_lists= 1
seed_per_file = 200
seed_limit = 2000
update_limit = 2000
list_limit = 200
verbose = 1
        """ % (sys.argv[0],sys.argv[0]))

if __name__ == '__main__':
  parameters = {"instance":timefunctions.datestamp(),"dir_log":"log","dir_seeds":"seeds","db_server":"localhost","db_database":"twaler","db_username":"snorgadmin","db_password":"snorg321","seed_userinfo":1,"seed_tweets":1,"seed_friends":1,"seed_listmemberships":1,"seed_lists":1,"seed_per_file":200,"seed_limit":2000,"update_limit":2000,"list_limit":200,"verbose":1}
  int_params = ["seed_userinfo","seed_tweets","seed_friends","seed_listmemberships","seed_lists","seed_per_file","seed_limit","list_limit","update_limit","verbose"]
  conf = parse_arguments(usage,parameters,int_params);
  generator = generate_seeds(**conf)
  generator.generate()