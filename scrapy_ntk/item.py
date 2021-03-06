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

# database meta
STRING_FIELDS = frozenset([URL, FINGERPRINT, TEXT, TAGS, HEADER, MEDIA, ERRORS])
DATE_FIELDS = frozenset([DATE])

CLASS_NAME = 'Article'
ITEM_CLASS_NAME = f'{CLASS_NAME}Item'
MODEL_CLASS_NAME = f'{CLASS_NAME}Model'


ArticleItem = type(ITEM_CLASS_NAME, (Item, ), {f: Field() for f in FIELDS})
