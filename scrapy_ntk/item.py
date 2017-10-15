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
INDEX = 'index'
DATE = 'date'


class ArticleItem(Item):
    header = Field()
    tags = Field()
    url = Field()
    text = Field()

    # non-public fields
    index = Field()
    date = Field()
