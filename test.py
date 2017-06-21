import time


import pymongo
from pymongo import MongoClient
url_path = 'Lkosh/tasks'

client = MongoClient()
db = client.http_pages_database
posts = db.posts
url = []
count = 0
for line in open(url_path):
	if count > 1000:
		break
	line = line.split('\n')[0]
	count += 1
	url.append("http://" + line)
t1 = time.clock()
print t1
for new_url in url:

	posts.find_one({"url":new_url})
t2 = time.clock()
print t2
print t2-t1