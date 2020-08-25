import requests
import string
import random
from datetime import datetime
from handleFileType import handle_html
from handleFileType import other_content_types
import time

#Creting an HTMl file with a random file name
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

# function to crawl data
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

            # setting html to content for easy writing without errors
            html_text = resp.content

            # if response is html then crawl as only html contains links
            if content_type == "text/html":
                handle_html(url, html_text, http_status, collection, content_type, content_length, new)
            else:
                # if not html, handle it with the corresponding content type
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

        # if data limit is reached
        if collection.count_documents({})>MAX_DATA_LIMIT:
            print("Maximumm Links Limit Exceeded")
            return
