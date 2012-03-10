#!/usr/bin/python2.6

import os
import sys

import misc
from misc import write_to_files

class Generator():
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        dir_seeds = self.config['dir_seeds']
        try:
            if not os.path.exists(dir_seeds):
                os.makedirs(dir_seeds)
            self.db = misc.mysql_db(self.config['db_server'],
                    self.config['db_username'],
                    self.config['db_password'],
                    self.config['db_database'], self.logger)
        except Exception as e:
            self.logger.error(str(e))

    def generate(self):
        self.generate_newSeeds()
        self.generate_updates()

    def generate_newSeeds(self):
        try:
            # TODO improve our naive seed generation method
            #---------------------------------
            # select the top _seed_limit_ most re-occuring friend that
            # has yet to be crawled
            stmt = ("SELECT friend_id FROM friends WHERE friend_id NOT IN "
                    "(SELECT user_id FROM users_update) "
                    "group by friend_id order by count(*) desc limit %s" %
                    self.config['seed_limit'])
            self.db.execute(stmt)
            self.logger.debug("MySQL generate_users Query Complete")
            seedType = 'utf'

            #---------------------------------
            # fetch results and write to new seed file
            results = self.db.cursor.fetchall()
            timestamp = misc.timefunctions.datestamp()
            write_to_files(results, 'seeds_' + timestamp,
                           self.config['seed_per_file'], seedType)
        except Exception as e:
            self.logger.error(str(e))

    def generate_updates(self):
        try:
            stmt = ("SELECT user_id FROM users_update Order by info_updated "
                    "desc limit %s" % self.config['seed_limit'])
            self.db.execute(stmt)
            self.logger.debug("MySQL generate_users Query Complete")
            seedType = 'utf'

            #---------------------------------
            # fetch results and write to new seed file
            results = self.db.cursor.fetchall()
            timestamp = misc.timefunctions.datestamp()
            write_to_files(results, 'update_' + timestamp,
                           self.config['seed_per_file'], seedType)
        except Exception as e:
            self.logger.error(str(e))
