from celery import group
import pandas as pd

from url_scraper import scrape_data
url_path = './tasks'

f = pd.read_csv('http_pages.csv')
#print f
read_urls = list(f.iloc[:, 0])
#read_urls += ['http://init-p01st.push.apple.com']

jobs = group(scrape_data.s(line, read_urls) for line in open(url_path))
result = jobs.apply_async()