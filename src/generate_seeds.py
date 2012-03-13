#!/usr/bin/python2.6

import os
import json
import logging

import misc

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
            misc.write_to_files(results, 'seeds_' + timestamp,
                           self.config['seed_per_file'], seedType)
        except Exception as e:
            self.logger.error(str(e))


def main():
    # Load global configurations
    fp = open('config.json')
    config = json.load(fp)
    fp.close()

    # Setup logger
    formatter = logging.Formatter(
            '%(asctime)-6s: %(funcName)s(%(filename)s:%(lineno)d) - '
            '%(levelname)s - %(message)s')
    consoleLogger = logging.StreamHandler()
    consoleLogger.setLevel(logging.DEBUG)
    consoleLogger.setFormatter(formatter)
    logging.getLogger('').addHandler(consoleLogger)
    logger = logging.getLogger('')
    logger.setLevel(logging.DEBUG)

    # Generate seeds
    generator = Generator(config, logger)
    generator.generate()


if __name__ == "__main__":
    main()
