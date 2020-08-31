import requests
import string
import random
from datetime import datetime
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

