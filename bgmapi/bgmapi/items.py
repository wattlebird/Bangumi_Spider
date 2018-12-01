# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class User(scrapy.Item):
    name = scrapy.Field()
    nickname = scrapy.Field()
    uid = scrapy.Field()
    joindate = scrapy.Field()
    activedate = scrapy.Field()


class Record(scrapy.Item):
    ## First five items are required.
    nickname = scrapy.Field()
    name = scrapy.Field()
    uid = scrapy.Field()
    typ = scrapy.Field()
    iid = scrapy.Field() #name and id together forms primary key.
    state = scrapy.Field()
    adddate = scrapy.Field()
    ## Following three are optional.
    rate = scrapy.Field()
    tags = scrapy.Field()
    comment = scrapy.Field()

class Subject(scrapy.Item):
    subjectid = scrapy.Field()
    subjecttype = scrapy.Field()
    subjectname = scrapy.Field()
    order = scrapy.Field() # may be None
    # The following are all optional
    rank = scrapy.Field()
    votenum = scrapy.Field()
    favnum = scrapy.Field()
    date = scrapy.Field()

    #staff = scrapy.Field() # feature list!
    staff = scrapy.Field() #map
    staff['serializer'] = lambda x: ";".join(":".join([k, ",".join(v)]) for k, v in x.items())
   