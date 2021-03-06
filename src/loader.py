#!/usr/bin/python2.6
import os
import sys
import json
import logging

class Loader():
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.prefix = ('mysql -u %s -p%s %s -e ' %
                (self.config['db_username'], self.config['db_password'],
                 self.config['db_database']))

    """Bulk Load a File"""
    def dump(self, table, filename, replace=True):
        if replace:
            replace_str = "REPLACE"
        else:
            replace_str = "IGNORE"
        stmt = ("LOAD DATA LOCAL INFILE \"%s\" %s INTO TABLE %s FIELDS "
                "TERMINATED BY \"\\t\" LINES TERMINATED BY \"\\n\"" %
                (filename, replace_str, table))
        self.execute(stmt)

    """Bulk Load + Update by loading to a temporary table first"""
    def dump_and_update(self, table, filename, update_keys):
        # Clear out temp table
        stmt = "DELETE FROM temp." + table
        self.execute(stmt)
        # Bulk load to table
        stmt = ("LOAD DATA LOCAL INFILE \"%s\" INTO TABLE temp.%s FIELDS "
                "TERMINATED BY \"\\t\" LINES TERMINATED BY \"\\n\"" %
                (filename, table))
        self.execute(stmt)
        # Insert/update to official table
        stmt = ("INSERT INTO %s SELECT * FROM temp.%s ON DUPLICATE KEY "
                "UPDATE " % (table, table))
        for key in update_keys:
            stmt += "%s= temp.%s.%s,"%(key, table, key)
        stmt = stmt[:-1]        # wipe out the extra comma
        self.execute(stmt)
        #delete temp table
        stmt = "DELETE FROM temp."+table
        self.execute(stmt)

    def execute(self, stmt):
        cmd = "%s '%s'" % (self.prefix, stmt)
        os.system(cmd)
        self.logger.debug("executed:" + stmt)

    def load(self, dir_data):
        files = os.listdir(dir_data)

        #load each file that corresponds to the table name in the directory
        for filename in files:
            filepath = os.path.join(dir_data, filename)
            if (filename == 'users.tsv'):
                self.dump("users", filepath)
            if (filename == 'tweets.tsv'):
                self.dump("tweets", filepath)
            if (filename == 'mentions.tsv'):
                self.dump("mentions", filepath)
            if (filename == 'urls.tsv'):
                self.dump("urls", filepath)
            if (filename == 'hashes.tsv'):
                self.dump("hashes", filepath)
            if (filename == 'hashtags.tsv'):
                self.dump("hashtags", filepath)
            if (filename == 'friends.tsv'):
                self.dump_and_update("friends", filepath, ["date_last"])
            if (filename == 'users_update.tsv'):
                self.dump_and_update("users_update", filepath, [
                    "info_updated", "tweet_updated", "friend_updated"])


def main():
    # Get cache path from parameter
    if len(sys.argv) < 2:
        print('Usage:%s cache/2012.03.10.10.10.10' % sys.argv[0])
        sys.exit(0)
    if not os.path.exists(sys.argv[1]):
        print('Usage:%s cache/2012.03.10.10.10.10' % sys.argv[0])
        sys.exit(0)

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

    # Load processed tsvs into mysql database
    loader = Loader(config, logger)
    loader.load(sys.argv[1])


if __name__ == "__main__":
    main()
