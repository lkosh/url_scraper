from __future__ import absolute_import, unicode_literals

import pandas as pd
import requests
from celery import Celery
import requests
requests.adapters.DEFAULT_RETRIES = 5
import pandas as pd
from .celery import app
from billiard import current_process


app = Celery('url_scraper', broker='pyamqp://guest@localhost//')


url_path = './tasks'

urls = []
url_count = 0
status_file = open('response_code' , 'a+')
output = open('http_pages', 'a')
new_url = []
labels = ['url', 'code', 'page']
read_urls = []

@app.task

#def text_only(text):
#    return text

def scrape_data(line, read_urls, db):
    pages= []
    response_codes = []
    urls = []

    line = line.split('\n')[0]
    new_url = "http://" + line
    if new_url in read_urls:
        return _,_,_
    
    try:
        r = requests.get(new_url)

    except:
        print ('error')

    response_codes.append(str(r.status_code))
    urls.append(new_url)
    if r.status_code >=300:
		pages.append('')
    else:
        pages.append(r.text)


	
	with open('/home/lena/mts/Lkosh/http_pages' + str(current_process().index) + '.csv', 'a') as f:
    		#print len(urls), len(response_codes), len(pages)
            l = [urls, response_codes, pages]
            df = pd.DataFrame({'url': urls, 'codes':response_codes, 'page': pages})
            
            #df = pd.DataFrame.from_items(l, columns = labels)
            df.to_csv(f, header=False, columns = ['url', 'codes', 'page'])
            f.flush()


    #df = pd.DataFrame({'url': urls, 'codes':response_codes, 'page': pages})
        
    return new_url, str(r.status_code), r.text 

"""
urls = []
response_codes = []
pages = []
for line in  open(url_path) :
    line = line.split('\n')[0]
    new_url = "http://" + line
    
    if new_url in read_urls:
        continue
    url_count += 1
    print (new_url)
    try:
        scrape_data(new_url)
    except:
        print ('error')
    #if url_count ==30:
    #    break
   
    if url_count % 10 == 0:
        with open('http_pages.csv', 'a') as f:
            print len(urls), len(response_codes), len(pages)
            l = [urls, response_codes, pages]
            #df = pd.DataFrame.from_items(l, columns = labels)
            df = pd.DataFrame({'url': urls, 'codes':response_codes, 'page': pages})
            df.to_csv(f, header=False, columns = ['url', 'codes', 'page'])
            f.flush()
            urls = []
            response_codes = []
            pages = []
            """