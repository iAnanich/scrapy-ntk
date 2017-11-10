# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


HEADER = 'header'
TAGS = 'tags'
URL = 'url'
TEXT = 'text'
MEDIA = 'media'
FINGERPRINT = 'fingerprint'
DATE = 'date'
ERRORS = 'errors'


class ArticleItem(Item):
    header = Field()
    tags = Field()
    url = Field()
    text = Field()
    media = Field()

    # non-public fields
    fingerprint = Field()
    date = Field()
    errors = Field()
