import requests
import time
import threading
from datetime import datetime
from logger import logger
from crawler_utils import *
from handleFileType import *
from pymongo import MongoClient

#CONSTANTS
MAX_DATA_LIMIT=5000
CRAWL_AFTER=datetime(2020,8,23)-datetime(2020,8,22)
DELAY_TIME=5
ROOT_URL="https://flinkhub.com/"

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

# crawl data
def crawl_data(data_list,DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection,new=True):

    for data in data_list:
        url=data['link']
        try:
            resp=requests.get(url)
            # obtain data
            html_text = resp.text
            http_status = resp.status_code
            headers = resp.headers

            # if http status is grater than 400 implies client side error or server side error
            if http_status >= 400:
                # for updating not crawled data new is true and for crawling old data new is false
                if new:
                    update_collection(url, collection, http_status=http_status)
                else:
                    update_collection_old(url, collection, http_status=http_status)
                continue

            try:
                content_length = int(headers['content-length'])
            # if content length not present
            except KeyError:
                content_length = len(html_text)

            content_type = headers['content-type'].split(";")
            content_type = content_type[0]
            # setting html to content for easy wirting without errors
            html_text = resp.content
            # if responce is html then crawl as only html contains links
            if content_type == "text/html":
                handle_html(url, html_text, http_status, collection, content_type, content_length, new)
            # if responce is not html then call other content type
            else:
                other_content_types(url, collection, http_status, content_length, content_type, html_text)
            time.sleep(DELAY_TIME)

        # network error then marked as crawled and crawl after 24 hr
        except (requests.ConnectionError ,requests.ConnectTimeout ,requests.HTTPError):
            # if new then update with changing created at from none to current date
            if new:
                update_collection(url,collection)
            # else do not update created at
            else :
                update_collection_old(url,collection)
            continue


        # if data limit exceed
        if collection.count_documents({})>MAX_DATA_LIMIT:
            print("Maximumm Links Limit Exceeded")
            return

#Crawler
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