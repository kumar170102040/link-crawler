import time
import threading
from bs4 import BeautifulSoup
from datetime import datetime
from logger import logger
from cfg import config
from crawlerUtils import crawl_data

from pymongo import MongoClient

#CONSTANTS
MAX_DATA_LIMIT=5000
CRAWL_AFTER=datetime(2020,8,23)-datetime(2020,8,22)
DELAY_TIME=5
ROOT_URL= config["ROOT_URL"]

#Connecting to MongoDB
cluster=MongoClient(port=27017)
db=cluster['web_crawler']

# if webcrawler already present then drop(helpful during testing)
db.webcrawler.drop()
collection=db['web_crawler']

#Adding Root URL to the MongoDB
root_data={"link":ROOT_URL,
            "source_link":"rooturl",
            "is_crawled":False,
            "last_crawl_dt":None,
            "Responce_status":None,
            "content_type":None,
            "content_length":None,
            "file_path":None,
            "created_at":None}

collection.insert_one(root_data)


# creating thread class to use with threading library
class thread_crawl(threading.Thread):
    def __init__(self, thread_id,data_list,DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection,new=True):
        threading.Thread.__init__(self)
        self.thread_id=thread_id
        self.data_list=data_list
        self.DELAY_TIME=DELAY_TIME
        self.CRAWL_AFTER=CRAWL_AFTER
        self.MAX_DATA_LIMIT=MAX_DATA_LIMIT
        self.collection=collection
        self.new=new
    def run(self):
        crawl_data(self.data_list,self.DELAY_TIME,self.CRAWL_AFTER,self.MAX_DATA_LIMIT,self.collection,self.new)

# creating and handling threads
class start_threads:
    def __init__(self,data_list,collection,DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,NO_OF_THREADS=5,new=True):
        self.collection=collection
        self.data_list=data_list
        self.DELAY_TIME=DELAY_TIME
        self.CRAWL_AFTER=CRAWL_AFTER
        self.MAX_DATA_LIMIT=MAX_DATA_LIMIT
        self.NO_OF_THREADS=NO_OF_THREADS
        self.new=new
        self.threads={}
    def start(self):
        len_of_data=len(self.data_list)
        thread_dividing=int(len_of_data/self.NO_OF_THREADS)
        for i in range(self.NO_OF_THREADS):
            self.threads[i]=thread_crawl(1,self.data_list[thread_dividing*i:thread_dividing*(i+1)],self.DELAY_TIME,self.CRAWL_AFTER,self.MAX_DATA_LIMIT,self.collection,self.new)
        for i in range(self.NO_OF_THREADS):
            self.threads[i].start()
    def is_not_complete(self):
        for i in range(self.NO_OF_THREADS):
            if self.threads[i].is_alive():
                return True
            return False


# function to save files

#Crawler
def crawler:
    while 1:
        # crawl for not crawled links
        not_crawled_links = list(collection.find({"is_crawled":False}))
        # if data is very less then no point of multithreading(i.e during first run only 1 data will be present)
        len_of_data = len(not_crawled_links)
        if len_of_data<5:
            crawl_data(not_crawled_links,DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection)
        # multithread work
        else :
            mythread= start_threads(data_list=not_crawled_links,collection=collection,DELAY_TIME=DELAY_TIME,CRAWL_AFTER=CRAWL_AFTER,MAX_DATA_LIMIT=MAX_DATA_LIMIT,NO_OF_THREADS=5)
            mythread.start()
            # wait until all theads are completed
            while mythread.is_not_complete():
                time.sleep(1)  # wait for one second and check again if threads are complete

        if collection.count_documents({})>MAX_DATA_LIMIT:
            print("Maximum Links Limit Exceeded")
            while collection.count_documents({})>MAX_DATA_LIMIT:
                time.sleep(10)  # wait for 10 sec in expecting data was cleaned by user


        # crawl for data that donot crawled for last one day
        daybefore=datetime.now()-CRAWL_AFTER
        old_data=list(collection.find({"last_crawl_dt":{"$lt":daybefore}}))
        if len(old_data)<5:
            crawl_data(old_data,DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection,new=False)
        else :
            mythread= start_threads(data_list=old_data,collection=collection,DELAY_TIME=DELAY_TIME,CRAWL_AFTER=CRAWL_AFTER,MAX_DATA_LIMIT=MAX_DATA_LIMIT,NO_OF_THREADS=5,new=False)
            mythread.start()
            while mythread.is_not_complete():
                time.sleep(1)  # wait for one second and check again if threads are complete
        if collection.count_documents({})>MAX_DATA_LIMIT:
            print("Maximum Links Limit Exceeded")
            while collection.count_documents({})>MAX_DATA_LIMIT:
                time.sleep(10)  # wait for 10 sec in expecting data was cleaned by user