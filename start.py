from celery import group
import pandas as pd
import os
from Lkosh.url_scraper import scrape_data
import pymongo
from pymongo import MongoClient
#elerycofig

url_path = 'Lkosh/tasks'





jobs = group(scrape_data.s(line) for line in open(url_path))
result = jobs.apply_async()
#for v in result.collect():

