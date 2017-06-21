from __future__ import absolute_import, unicode_literals
from celery import Celery

app = Celery('Lkosh',
             broker='mongodb://',
             backend='mongodb://',
             include=['Lkosh.url_scraper'])


app.conf.update(
    result_backend='mongodb://localhost:27017/http_pages_database',
    result_serializer='json',
    result_expires=0
)

#app.config_from_pycode('config.celeryconfig')
if __name__ == '__main__':
    app.start()
