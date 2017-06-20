from celery import group
import pandas as pd
import os
from Lkosh.url_scraper import scrape_data
import pymongo
from pymongo import MongoClient

url_path = 'Lkosh/tasks'
print "start"
read_urls = []

filename_list = ['Lkosh/http_pages0.csv', 'Lkosh/http_pages1.csv', 'Lkosh/http_pages2.csv', 'Lkosh/http_pages3.csv']
for filename in filename_list:
	if os.path.getsize(filename)> 0:
		f = pd.read_csv(filename)
	#print f
		read_urls += list(f.iloc[:, 0])
""" DATABASE """
#client = MongoClient()
#db = client.http_pages_database

#read_urls += ['http://init-p01st.push.apple.com']
print "read"
jobs = group(scrape_data.s(line, read_urls) for line in open(url_path))
result = jobs.apply_async()
'''
with open('/home/lena/mts/Lkosh/http_pages.csv', 'a') as f:
		#print len(urls), len(response_codes), len(pages)
		l = [urls, response_codes, pages]
        #df = pd.DataFrame.from_items(l, columns = labels)
		url, status, text = jobs.get() #pd.DataFrame({'url': new_url, 'codes':str(r.status_code), 'page': r.text})
		print url
		df.to_csv(f, header=True, columns = ['url', 'codes', 'page'])
		f.flush()
print result
'''