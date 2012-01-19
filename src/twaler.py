#!/usr/local/bin/python3
import os
import shutil
import sys
import signal
import multiprocessing
import crawl
import process_crawl
import load_crawl
import generate_seeds

from config import parse_arguments
from config import timefunctions
from config import logger
   
class twaler:
  def __init__(self,dir_cache,dir_log,dir_seeds,dir_seedsdone,**kwargs):
    self.name = "twaler"  
    self.configurations = kwargs
    self.dir_seeds = dir_seeds
    self.dir_seedsdone = dir_seedsdone
    if not os.path.exists(dir_seedsdone):
        os.makedirs(dir_seedsdone)
    self.dir_cache = dir_cache
    if not os.path.exists(dir_cache):
        os.makedirs(dir_cache)
    self.dir_log = dir_log
    if kwargs['verbose']: 
      self.log = logger(self.name,dir_log).verbose_log
    else:
      self.log = logger(self.name,dir_log).log
    
  def twale(self):
    #create a child process
    self.child = os.fork()
    if self.child == 0:
      self.twalerloop()
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
    sys.exit() 

  def crawl(self,crawl_instance,seed_file):
    #set new parameters for crawling
    #a folder under cache will be created with the datestamp as the crawl_instance
    seedFileCacheDir = os.path.join(self.dir_cache,crawl_instance)
    self.configurations["seed_file"] = os.path.join(self.dir_seeds,seed_file)
    self.configurations["dir_cache"] = seedFileCacheDir
    self.configurations["instance"] = crawl_instance
    self.configurations["dir_log"] = os.path.join(seedFileCacheDir,"log")
    
    #CRAWL the given instance
    self.log("Crawling " + seed_file)
    crawler = crawl.crawler(**self.configurations)
    crawler.crawloop()
    
  def processAndLoad(self,crawl_instance):
    #a folder under cache will be created with the datestamp as the crawl_instance
    seedFileCacheDir = os.path.join(self.dir_cache,crawl_instance)
    self.configurations["dir_cache"] = seedFileCacheDir
    self.configurations["instance"] = crawl_instance
    self.configurations["dir_log"] = os.path.join(seedFileCacheDir,"log")
    self.configurations["dir_processed"] = os.path.join(seedFileCacheDir,"processed_crawl")
        
    #PROCESS the given instance
    self.log("Processing instance " + crawl_instance)
    processor = process_crawl.process_crawl(**self.configurations)
    processor.process_loop()
    
    #LOAD the given instance
    self.log("Loading instance " + crawl_instance)
    loader = load_crawl.load_crawl(**self.configurations)
    loader.load_loop()
    
  def generateSeeds(self):
    self.configurations["instance"] = timefunctions.datestamp()
    self.configurations["dir_seeds"] = self.dir_seeds
    self.configurations["dir_log"] = self.dir_log

    generator = generate_seeds.generate_seeds(**self.configurations)
    generator.generate()
    
    
  def twalerloop(self):
    while True:
      #CHECK FOR SEEDS        
      seedFiles = os.listdir(self.dir_seeds)
        
      #GENERATE SEEDS (if no more seeds)
      if not(seedFiles):
        self.log("Seed Folder Empty")
        self.generateSeeds()
        self.log("Generate seeds complete")
        
      processAndLoadProcesses = []
      for seed_file in seedFiles:
        crawl_instance = timefunctions.datestamp() 
        #CRAWL (continuously)
        self.crawl(crawl_instance,seed_file)
        #PROCESS AND LOAD (generate subprocess)
        p = multiprocessing.Process(target=self.processAndLoad, args=(crawl_instance,))
        processAndLoadProcesses.append(p)
        p.start()
        #Move the seed file into seeds_done, and also the crawl instance folder
        shutil.copy(os.path.join(self.dir_seeds,seed_file), os.path.join(os.path.join(self.dir_cache,crawl_instance),"seed["+seed_file+"].txt"))
        shutil.move(os.path.join(self.dir_seeds,seed_file), os.path.join(self.dir_seedsdone,seed_file))
          
      #wait for this round to finish
      for p in processAndLoadProcesses:
        p.join()
    
def usage():
  print("""\nUsage: %s [manual parameters] <config file>\n
Contents in configuration file:
*Values listed here are default values, used if parameter is unspecified
*Can also be overwritten with parameters 
*ie) %s --seed_file=seedfile.txt config.txt

dir_seeds= seeds
dir_seedsdone= seedsdone
dir_cache= cache
dir_log= log
verbose = 0

#---crawl---#
crawl_numOfThreads= 10

#---process_crawl---#
process_userinfo= 1
process_friends= 1
process_memberships= 1  
process_tweets= 1
process_listmembers= 1
extract_mentions= 1
extract_urls= 1
extract_hashes=1

#---load_crawl---#
db_server= localhost
db_database= twaler
db_username= snorgadmin
db_password= snorg321

#---generate_seeds---#
seed_userinfo = 1
seed_tweets= 1
seed_friends= 1
seed_listmemberships= 1
seed_lists= 1
seed_per_file = 200
seed_limit = 2000
update_limit = 2000
list_limit = 100
        """ % (sys.argv[0],sys.argv[0]))

if __name__ == '__main__':
  parameters = {"dir_cache":"cache","dir_log":"log", "dir_seeds":"seeds","dir_seedsdone":"seedsdone", "verbose":0}
  int_params = ["verbose"]
  #parameters from crawl
  parameters.update({"seed_file":"<in dir_seeds>","crawl_numOfThreads":10})
  int_params.extend(["crawl_numOfThreads"])
  #parameters from process_crawl
  parameters.update({"process_to_db":0,"dir_processed":"<in cache instance folders>","process_userinfo":1,"process_friends":1,"process_memberships":1,"process_tweets":1,"process_listmembers":1,"extract_mentions":1,"extract_urls":1,"extract_hashes":1})
  int_params.extend(["process_userinfo","process_friends","process_memberships",  "process_tweets","process_listmembers","extract_mentions","extract_urls","extract_hashes"])
  #parameters from load_crawl
  parameters.update({"db_server":"localhost","db_database":"twaler","db_username":"snorgadmin","db_password":"snorg321"})
  #parameters from generate_seeds
  parameters.update({"seed_userinfo":1,"seed_tweets":1,"seed_friends":1,"seed_listmemberships":1,"seed_lists":1,"seed_per_file":200,"seed_limit":2000,"update_limit":2000,"list_limit":100})
  int_params.extend(["seed_userinfo","seed_tweets","seed_friends","seed_listmemberships","seed_lists","seed_per_file","seed_limit","update_limit","list_limit"])
  
  conf = parse_arguments(usage,parameters,int_params);
  t = twaler(**conf)
  t.twale()
