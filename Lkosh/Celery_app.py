from __future__ import absolute_import, unicode_literals
from celery import Celery

app = Celery('Lkosh',
             broker='amqp://',
             backend='amqp://',
             include=['url_scraper'])

# Optional configuration, see the application user guide.
app.conf.update(
    result_expires=3600,
)

if __name__ == '__main__':
    app.start()
