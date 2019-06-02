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
    group = scrapy.Field()

class Subject(scrapy.Item):
    subjectid = scrapy.Field()
    subjecttype = scrapy.Field()
    subjectname = scrapy.Field()
    subjectname_cn = scrapy.Field()
    order = scrapy.Field() # may be None
    # The following are all optional
    rank = scrapy.Field()
    votenum = scrapy.Field()
    favnum = scrapy.Field()
    date = scrapy.Field()

    #staff = scrapy.Field() # feature list!
    staff = scrapy.Field() #map
    staff['serializer'] = lambda x: ";".join(":".join([k, ",".join(v)]) for k, v in x.items())
