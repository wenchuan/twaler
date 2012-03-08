#!/usr/local/bin/python3

import os
import sys

from misc import parse_arguments
from misc import write_to_files
from misc import timefunctions
from misc import mysql_db
from misc import Logger

class Generator():
    def __init__(self, instance, dir_log, dir_seeds, db_username, db_password,
                 db_database, db_server, seed_userinfo, seed_tweets,
                 seed_friends, seed_listmemberships, seed_lists,
                 seed_per_file, seed_limit, update_limit, list_limit,
                 verbose, **kwargs):
        try:
            self.name = "generate_seeds"
            self.dir_seedout = dir_seeds
            if not os.path.exists(dir_seeds):
                os.makedirs(dir_seeds)
            if verbose:
                self.log = Logger(self.name, dir_log).verbose_log
            else:
                self.log = Logger(self.name, dir_log).log
            self.instance = instance
            self.instanceTimeStamp = timefunctions.instanceToSqlTime(instance)
            self.mysql_db = mysql_db(db_server, db_username, db_password,
                                     db_database, self.log)
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
        self.generate_newSeeds()
        self.generate_updates()

    def generate_newSeeds(self):
        try:
            # TODO improve our naive seed generation method
            #---------------------------------
            # select the top (seed_limit) most re-occuring friend that
            # has yet to be crawled
            stmt = ("SELECT friend_id FROM friends WHERE friend_id NOT IN "
                    "(SELECT user_id FROM users_update) "
                    "group by friend_id order by count(*) desc limit " +
                    str(self.seed_limit))
            self.mysql_db.execute(stmt)
            self.log("MySQL generate_users Query Complete")
            seedType = ''
            if self.seed_userinfo:
                seedType += 'u'
            if self.seed_tweets:
                seedType += 't'
            if self.seed_friends:
                seedType += 'f'
            if self.seed_listmemberships:
                seedType += 'l'

            #---------------------------------
            # fetch results and write to new seed file
            results = self.mysql_db.cursor.fetchall()
            write_to_files(results, 'seeds_' + self.instance,
                           self.seed_per_file, seedType)
        except Exception as e:
            self.log(str(e))

    def generate_updates(self):
        try:
            stmt = "SELECT user_id FROM users_update Order by info_updated desc limit " + str(self.seed_limit)
            self.mysql_db.execute(stmt)
            self.log("MySQL generate_users Query Complete")
            seedType = ''
            if self.seed_userinfo:
                seedType += 'u'
            if self.seed_tweets:
                seedType += 't'
            if self.seed_friends:
                seedType += 'f'
            if self.seed_listmemberships:
                seedType += 'l'

            #---------------------------------
            # fetch results and write to new seed file
            results = self.mysql_db.cursor.fetchall()
            write_to_files(results, 'update_' + self.instance,
                           self.seed_per_file, seedType)
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
    generator = Generator(**conf)
    generator.generate()
