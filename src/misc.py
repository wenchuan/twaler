#!/usr/local/bin/python2.6

from __future__ import print_function

import time
import datetime
import os
import gzip
import re
import codecs
import MySQLdb

from contextlib import closing


class timefunctions:
    """Functions that handle time format conversions"""
    @staticmethod
    def datestamp():
        """Return a string representing the time"""
        return time.strftime("%Y.%m.%d.%H.%M.%S", time.localtime())

    @staticmethod
    def sqlTime():
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    @staticmethod
    def jsonToSqlTime(dtstr):
        # N.B. The time used here is GMT time!!
        # not all platforms support '%z' with strptime, manually parse it
        dt = datetime.datetime.strptime(dtstr, "%a %b %d %H:%M:%S +0000 %Y")
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def instanceToSqlTime(dtstr):
        dt = datetime.datetime.strptime(dtstr, "%Y.%m.%d.%H.%M.%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def xmlToSqlTime(dtstr):
        # N.B. The time used here is GMT time!!
        # not all platforms support '%z' with strptime, manually parse it
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    @staticmethod
    def unixToSqlTime(dtstr):
        t = time.gmtime(float(dtstr))
        return time.strftime("%Y-%m-%d %H:%M:%S",t)


class CacheAccessor():
    """Cache files writer/reader"""
    def __init__(self, cache_dir, logger):
        self.cache_dir = cache_dir
        self.pseudoseconds = 0
        self.logger = logger

    def get_crawl_dir(self, basepath, uid, create=False, listname=None):
        """
        Return and create (if needed), the path associated with this user id
        """
        uid = uid.zfill(3)
        cache_path = os.path.join(basepath, uid[-1], uid[-2], uid[-3], uid)
        if listname:
            cache_path = os.path.join(cache_path, "lists", listname)
        if not os.path.exists(cache_path) and create:
            os.makedirs(cache_path)
        return cache_path

    def get_cache_dir(self, uid, create=False, listname=None):
        """
        Return and create (if needed), the path associated with this user id
        """
        return self.get_crawl_dir(self.cache_dir, uid, create=create,
                                  listname=listname)

    def check_cache(self, uid):
        """
        return last read tweet id if cache includes it otherwise return -1
        """
        cache_path = self.get_cache_dir(uid, create=False)
        last_checked = os.path.join(cache_path, "last_checked")
        if os.path.exists(last_checked):
            with closing(open(last_checked, "r")) as fin:
                id = int(fin.read())
                return id
        return -1

    def store_in_cache(self, request_type, uid, header, data,
                       listname=None, datagzipped=False):
        """write http headers and data to the user specific directory
           datagzipped -  data is already gzipped"""
        #Get user specific directory of his files
        cache_path = self.get_cache_dir(uid, create=True, listname=listname)
        #form cache file base name
        #we're too fast, we need more differentiation
        self.pseudoseconds = (self.pseudoseconds + 1) % 100
        now = timefunctions.datestamp() + (".%03d" % self.pseudoseconds)
        cache_file = os.path.join(cache_path, request_type + ".%s." + now + ".gz")
        #write headers
        with closing(gzip.open(cache_file % "headers", "w")) as fout:
            fout.write(str.encode(header))
        #write data
        #data is already gzipped:
        if datagzipped:
            with closing(open(cache_file % "data", "wb")) as fout:
                fout.write(data)
        #data is not gzipped:
        else:
            with closing(gzip.open(cache_file % "data", "w")) as fout:
                fout.write(data)

    def set_cache_dir(self, cache_dir):
        self.cache_dir = cache_dir

    def get_infiles(self,cache_path = None, uid=None):
        if (not(cache_path) and not(uid)):
            return None
        if(uid and not(cache_path)):
            cache_path = self.get_cache_dir(uid)
        try:
            infiles = os.listdir(cache_path)
            return infiles
        except:
            self.logger.warning("can not list contents of directory " +
                    cache_path)
            return None

    def idqueue(self):
        #go through the crawl directory and get all the crawl_id folders
        for file in os.walk(self.cache_dir):
            filename = file[0]
            pattern = re.escape(self.cache_dir) + "/\d+/\d+/\d+/(\d*)"
            m = re.match(pattern, filename)
            if m:
                yield(m.group(1), filename)


class mysql_db():
    """Connector class for MySQL database"""
    def __init__(self, host, user, password, db, logger):
        try:
            self.conn = MySQLdb.connect(host, user, password, db)
            self.cursor = self.conn.cursor()
            self.logger = logger
            self.logger.debug("Connected to MySQL as " + user)
        except Exception as e:
            self.log("MySQL Connect Error:"+ str(e))

    def insert(self, table, values, updates=None):
        try:
            stmt = "INSERT INTO " + table + " VALUES("
            for _ in values:
                stmt += "%s,"
            stmt = stmt[:-1] + ")"
            if updates:
                stmt += " ON DUPLICATE KEY UPDATE "
                for key in updates:
                    stmt += key + "='" + updates[key] + "',"
                stmt = stmt[:-1]
            self.cursor.execute(stmt, values)
        except Exception as e:
            self.logger.error("MySQL Insert/Update Error:"+ str(e)  + "\n(stmt):" + stmt)

    def execute(self, stmt):
        try:
            self.cursor.execute(stmt)
        except Exception as e:
            self.logger.error("MySQL Execute Error:" + str(e) + "\n(stmt):" + stmt)

    def __del__(self):
        self.conn.close()


class file_db():
    """Class to write to file"""
    """Opens a table of filestreams and writes to them until close"""
    def __init__(self, dir_file, logger):
        try:
            self.dir_file = dir_file
            self.logger = logger
            self.logger.debug("Writing output files to directory " +
                    self.dir_file)
            self.fileStream = {}
        except Exception as e:
            self.logger.erro("File Error:"+ str(e))

    def insert(self, table, values, updates=None):
        try:
            if table not in self.fileStream:
                self.fileStream[table] = codecs.open(
                        os.path.join(self.dir_file, table+".tsv"), "w",
                        encoding='utf-8')
            print(*values, sep='\t', end='\n', file=self.fileStream[table])
        except Exception as e:
            self.logger.error("File write error:"+ str(e))

    def __del__(self):
        for table in self.fileStream:
            self.fileStream[table].close()

    def execute(self, stmt):
        return None


def write_to_files(seeds, file_prefix, id_per_file, seedtype='utf'):
    '''
    Dump a list of seeds into directory, started by file_prefix every
    file contains id_per_file ids, individual files are numbered by
    numbers, starting from zero
    '''
    suffix = cnt = 0
    seed_dir = 'seeds'      # N.B. need change?
    name = '%s_%s.txt' % (file_prefix, suffix)
    fp = open(os.path.join(seed_dir, name), 'w')
    for user in seeds:
        if cnt == id_per_file:
            # Open new file when one file is filled
            fp.close()
            suffix += 1
            name = '%s_%s.txt' % (file_prefix, suffix)
            fp = open(os.path.join(seed_dir, name), 'w')
            cnt = 0
        fp.write('%s\t%s\n' % (seedtype, user[0]))
        cnt += 1
    fp.close()
