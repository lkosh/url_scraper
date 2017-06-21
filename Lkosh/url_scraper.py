""" Time for 1000 queries - 0.26 """


from __future__ import absolute_import, unicode_literals

import pandas as pd
import requests
from celery import Celery
import requests
import pandas as pd
from .celery import app
from billiard import current_process
#import celerycofig
import pymongo
from pymongo import MongoClient

app = Celery('url_scraper', broker='mongodb://localhost:27017//')






client = MongoClient()


@app.task
def scrape_data(line):
    requests.adapters.DEFAULT_RETRIES = 5
    db = client.http_pages_database
    posts = db.celery_taskmeta

    pages= []
    response_codes = []
    

    line = line.split('\n')[0]
    new_url = "http://" + line
    if str(posts.find_one({"result.url":new_url})) != 'None' :
       return {}
    
    try:
        r = requests.get(new_url)

    except:
        print ('error')
        return {}
    response_codes.append(str(r.status_code))
  
    if r.status_code >=300:
		pages.append('')

    else:
        pages.append(r.text)


    post = {"url":new_url, "code": response_codes[0], "text": pages[0]}


    
    return post
   
