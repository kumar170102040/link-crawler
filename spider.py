import requests
import string
import random
import time
import threading
from bs4 import BeautifulSoup
from datetime import datetime

#from logger import logger
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


# function to save files
def write_to_file(file_name,html_text):
    try:
        with open("html_files/"+file_name,"wb") as f:
            f.write(html_text)
    except UnicodeEncodeError:
        file_name=None
    return file_name

# function to update collection with changing created at(for updating not crawled data)
def update_collection(url,collection, http_status=None,content_length=None,content_type=None,file_name=None):
    collection.update_one({"link":url},{"$set":{"is_crawled":True,
                                                "last_crawl_dt":datetime.now(),
                                                "Responce_status":http_status,
                                                "content_type":content_type,
                                                "content_length":content_length,
                                                "file_path":file_name,
                                                "created_at":datetime.now()}})

# function to update collection without changing created at(for updating old data)
def update_collection_old(url,collection, http_status=None,content_length=None,content_type=None,file_name=None):
    collection.update_one({"link":url},{"$set":{"is_crawled":True,
                                                "last_crawl_dt":datetime.now(),
                                                "Responce_status":http_status,
                                                "content_type":content_type,
                                                "content_length":content_length,
                                                "file_path":file_name}})

# function to create new link during crawling
def create_new_link(url,link,collection):
    collection.insert_one({"link":link, "source_link":url,
                            "is_crawled":False,"last_crawl_dt":None,
                            "Responce_status":None, "content_type":None,
                            "content_length":None, "file_path":None,
                            "created_at":None})

# function to generate random sting
def generate_random_string():
    ascii_letters = string.ascii_lowercase
    file_name = ''.join(random.choice(ascii_letters) for i in range(10))
    return file_name


# handel content type of applications
def handel_applications(content_type):
    file_name = generate_random_string()
    if content_type[11:] == "/pdf":
        file_name += ".pdf"
    elif content_type[11:] == "/json":
        file_name += ".json"
    elif content_type[11:] == "/xml":
        file_name += ".xml"
    elif content_type[11:] == "/javascript":
        file_name += ".js"
    elif content_type[11:] == "/zip":
        file_name += ".zip"
    elif content_type[11:] == "/x-7z-compressed":
        file_name += ".7z"
    elif content_type[11:] == "/vnd.mozilla.xul+xml":
        file_name += ".xul"
    elif content_type[11:] == "/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        file_name += ".xlsx"
    elif content_type[11:] == "/vnd.ms-excel":
        file_name += ".xls"
    elif content_type[11:] == "/xhtml+xml":
        file_name += ".xhtml"
    elif content_type[11:] == "/x-tar":
        file_name += ".tar"
    elif content_type[11:] == "/x-sh":
        file_name += ".sh"
    elif content_type[11:] == "/rtf":
        file_name += ".rtf"
    elif content_type[11:] == "/vnd.rar":
        file_name += ".rar"
    elif content_type[11:] == "/vnd.openxmlformats-officedocument.presentationml.presentation":
        file_name += ".pptx"
    elif content_type[11:] == "/vnd.ms-powerpoint":
        file_name += ".ppt"
    elif content_type[11:] == "/x-httpd-php":
        file_name += ".php"
    # for other file types leave
    else:
        file_name = None
    return file_name


# handel content type of audio
def handel_audio(content_type):
    file_name = generate_random_string()
    if content_type[5:] == "/acc":
        file_name += ".acc"
    elif content_type[5:] == "/ogg":
        file_name += ".ogg"
    elif content_type[5:] == "/opus":
        file_name += ".ogg"
    elif content_type[5:] == "/wav":
        file_name += ".wav"
    elif content_type[5:] == "/webm":
        file_name += ".webm"
    elif content_type[5:] == "/3gpp":
        file_name += ".3gp"
    elif content_type[5:] == "/3gpp2":
        file_name += ".3g2"
    else:
        file_name = None
    return file_name


# handel content type of text
def handel_text(content_type):
    file_name = generate_random_string()
    if content_type[4:] == "/plain":
        temp = url.split(".")
        file_name += "." + temp[-1]
    elif content_type[4:] == "/xml":
        file_name += ".xml"
    elif content_type[4:] == "/javascript":
        file_name += ".xml"
    elif content_type[4:] == "/csv":
        file_name += ".xml"
    elif content_type[4:] == "/css":
        file_name += ".xml"
    elif content_type[4:] == "/css":
        file_name += ".xml"
    else:
        file_name = None
    return file_name


# handel content of type image
def handel_image(content_type):
    file_name = generate_random_string()
    if content_type[5:] == "/vnd.microsoft.icon":
        file_name += ".ico"
    elif content_type[5:] == "/jpeg":
        file_name += ".jpg"
    elif content_type[5:] == "/bmp":
        file_name += ".bmp"
    elif content_type[5:] == "/gif":
        file_name += ".gif"
    elif content_type[5:] == "/png":
        file_name += ".png"
    elif content_type[5:] == "/svg+xml":
        file_name += ".svg"
    elif content_type[5:] == "/tiff":
        file_name += ".tiff"
    elif content_type[5:] == "/webp":
        file_name += ".webp"
    else:
        file_name = None
    return file_name


# handel videos
def handel_video(content_type):
    file_name = generate_random_string()
    if content_type[5:] == "/x-msvideo":
        file_name += ".avi"
    elif content_type[5:] == "/mpeg":
        file_name += ".mpeg"
    elif content_type[5:] == "/ogg":
        file_name += ".ogv"
    elif content_type[5:] == "/mp2t":
        file_name += ".ts"
    elif content_type[5:] == "/webm":
        file_name += ".webm"
    elif content_type[5:] == "/3gpp":
        file_name += ".3gp"
    elif content_type[5:] == "/3gpp2":
        file_name += ".3g2"
    else:
        file_name = None
    return file_name


# hadel types other than html
def other_content_types(url, collection, http_status, content_length, content_type, html_text, new=True):
    file_name = None

    # if it is application
    if content_type[:11] == "application":
        file_name = handel_applications(content_type)

    # if content type is audio
    elif content_type[:5] == "audio":
        file_name = handel_audio(content_type)

    # if content type is text
    elif content_type[:4] == "text":
        file_name = handel_text(content_type)

    # if content type is image
    elif content_type[:5] == "image":
        file_name = handel_image(content_type)

    # if content type is video
    elif content_type[:5] == "video":
        file_name = handel_video(content_type)

    if file_name != None:
        file_name = write_to_file(file_name, html_text)
        # for updating not crawled data new is true and for crawling old data new is false
        if new:
            update_collection(url=url, http_status=http_status, content_length=content_length,
                              content_type=content_type, file_name=file_name, collection=collection)
        else:
            update_collection_old(url=url, http_status=http_status, content_length=content_length,
                                  content_type=content_type, file_name=file_name, collection=collection)

# handel html
def handel_html(url, html_text, http_status, collection, content_type, content_length, new=True):
    soup = BeautifulSoup(html_text, 'html.parser')
    a_tags = soup.find_all("a")
    for tag in a_tags:
        link = tag.get("href")
        # clean link
        try:
            # if link is not http(or https) or starts with / it is not valid
            if link[:4] != "http" and link[0] != "/":
                # print(link,"is not a valid link")
                continue
            if link[0] == "/":
                link = url + link
            # links like example.com and example.com/ are same
            if link[-1] == "/":
                link = link[:-1]
        # if link is empty string
        except (IndexError, TypeError):
            # print(link,"is not a valid link")
            continue
        # if link not present in data base then add it
        if collection.find_one({"link": link}) == None:
            create_new_link(url, link, collection)

    file_name = generate_random_string()
    file_name += ".html"
    file_name = write_to_file(file_name, html_text)

    # for updating not crawled data new is true and for crawling old data new is false
    if new:
        update_collection(url=url, http_status=http_status, content_length=content_length, content_type=content_type,
                          file_name=file_name, collection=collection)
    else:
        update_collection_old(url=url, http_status=http_status, content_length=content_length,
                              content_type=content_type, file_name=file_name, collection=collection)
    # print(url,"is sucessfully crawled and data updated")

# crawl data
def crawl_data(data_list,DELAY_TIME,CRAWL_AFTER,MAX_DATA_LIMIT,collection,new=True):
    for data in data_list:
        url=data['link']
        try:
            resp=requests.get(url)
        # network error then marked as crawled and crawl after 24 hr
        except (requests.ConnectionError ,requests.ConnectTimeout ,requests.HTTPError):
            # if new then update with changing created at from none to current date
            if new:
                update_collection(url,collection)
            # else do not update created at
            else :
                update_collection_old(url,collection)
            continue
        # obtain data
        html_text=resp.text
        http_status=resp.status_code
        headers=resp.headers

        # if http status is grater than 400 implies client side error or server side error
        if http_status>=400:
            # for updating not crawled data new is true and for crawling old data new is false
            if new:
                update_collection(url,collection,http_status=http_status)
            else :
                update_collection_old(url,collection,http_status=http_status)
            continue
        try:
            content_length=int(headers['content-length'])
        # if content length not present
        except KeyError:
            content_length=len(html_text)
        content_type=headers['content-type'].split(";")
        content_type=content_type[0]
        # setting html to content for easy wirting without errors
        html_text=resp.content
        # if responce is html then crawl as only html contains links
        if content_type=="text/html":
            handel_html(url,html_text,http_status,collection,content_type,content_length,new)
        # if responce is not html then
        else :
            other_content_types(url,collection,http_status,content_length,content_type,html_text)
        time.sleep(DELAY_TIME)

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