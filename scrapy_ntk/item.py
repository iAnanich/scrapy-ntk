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

FIELDS = frozenset([URL, FINGERPRINT, TEXT, TAGS, DATE, HEADER, MEDIA, ERRORS])

ITEM_CLASS_NAME = 'ArticleItem'


ArticleItem = type(ITEM_CLASS_NAME, (Item, ), {f: Field() for f in FIELDS})
