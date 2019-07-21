# -*- coding: utf-8 -*-

import scrapy
import re
from bgm.items import Record, Index, Friend, User, SubjectInfo, Subject
from bgm.util import *
from scrapy.http import Request
import datetime
import json

mpa = dict([(i, None) for i in range(32)])

class UserSpider(scrapy.Spider):
    name = 'user'
    def __init__(self, *args, **kwargs):
        super(UserSpider, self).__init__(*args, **kwargs)
        if not hasattr(self, 'id_max'):
            self.id_max=400000
        if not hasattr(self, 'id_min'):
            self.id_min=1
        self.start_urls = ["http://mirror.bgm.rin.cat/user/"+str(i) for i in range(int(self.id_min),int(self.id_max))]

    def parse(self, response):
        if len(response.xpath(".//*[@id='headerProfile']"))==0:
            return
        user = response.xpath(".//*[@id='headerProfile']/div/div/h1/div[3]/small/text()").extract()[0][1:]
        nickname = response.xpath(".//*[@class='headerContainer']//*[@class='inner']/a/text()").extract()[0].translate(mpa)

        # Is blocked?
        if len(response.xpath("//ul[@class='timeline']/li"))==0:
            return;

        if not 'redirect_urls' in response.meta:
            uid = int(user)
        else:
            uid = int(response.meta['redirect_urls'][0].split('/')[-1])
        date = response.xpath(".//*[@id='user_home']/div[@class='user_box clearit']/ul/li[1]/span[2]/text()").extract()[0].split(' ')[0]
        date = parsedate(date)

        yield User(name=user, nickname=nickname, uid=uid, joindate=date)

class IndexSpider(scrapy.Spider):
    name='index'
    def __init__(self, *args, **kwargs):
        super(IndexSpider, self).__init__(*args, **kwargs)
        if not hasattr(self, 'id_max'):
            self.id_max=20000
        if not hasattr(self, 'id_min'):
            self.id_min=1
        self.start_urls = ["http://mirror.bgm.rin.cat/index/"+str(i) for i in range(int(self.id_min),int(self.id_max))]

    def parse(self, response):
        if len(response.xpath(".//*[@id='columnSubjectBrowserA']/div[1]/a"))==0:
            return
        indexid = response.url.split('/')[-1]
        indexid=int(indexid)
        creator = response.xpath(".//*[@id='columnSubjectBrowserA']/div[1]/a/@href").extract()[0].split('/')[-1]
        creator=str(creator).translate(mpa)
        td = response.xpath(".//*[@id='columnSubjectBrowserA']/div[1]/span/span[1]/text()").extract()[0]
        date = parsedate(td.split(' ')[0])
        if len(response.xpath(".//*[@id='columnSubjectBrowserA']/div[1]/span/span"))==2:
            favourite = response.xpath(".//*[@id='columnSubjectBrowserA']/div[1]/span/span[2]/text()").extract()[0]
            favourite = int(favourite)
        else: favourite = 0
        items = response.xpath(".//*[@id='columnSubjectBrowserA']/ul/li/@id").extract()
        items = [int(itm.split('_')[-1]) for itm in items]
        yield Index(indexid=indexid, creator=creator, favourite=favourite, date=date, items=items)

class RecordSpider(scrapy.Spider):
    name='record'
    def __init__(self, *args, **kwargs):
        super(RecordSpider, self).__init__(*args, **kwargs)
        if hasattr(self, 'userlist'):
            userlist = []
            with open(self.userlist, 'r') as fr:
                while True:
                    l = fr.readline().strip()
                    if not l: break;
                    userlist.append(l)
            self.start_urls = ["http://mirror.bgm.rin.cat/user/"+i for i in userlist]
        else:
            if not hasattr(self, 'id_max'):
                self.id_max=500000
            if not hasattr(self, 'id_min'):
                self.id_min=1
            self.start_urls = ["http://mirror.bgm.rin.cat/user/"+str(i) for i in range(int(self.id_min),int(self.id_max))]

    def parse(self, response):
        username = response.url.split('/')[-1]
        if (not response.xpath(".//*[@id='headerProfile']")) or response.xpath(".//div[@class='tipIntro']"):
            return
        uid = int(response.meta['redirect_urls'][0].split('/')[-1]) if 'redirect_urls' in response.meta else int(username)
        nickname = response.xpath(".//*[@class='headerContainer']//*[@class='inner']/a/text()").extract()[0].translate(mpa)

        date = response.xpath(".//*[@id='user_home']/div[@class='user_box clearit']/ul/li[1]/span[2]/text()").extract()[0].split(' ')[0]
        date = parsedate(date)

        yield User(name=username, nickname=nickname, uid=uid, joindate=date)

        if len(response.xpath(".//*[@id='anime']")):
            yield scrapy.Request("http://mirror.bgm.rin.cat/anime/list/"+username, callback = self.merge, meta = { 'uid': uid })

        if len(response.xpath(".//*[@id='game']")):
            yield scrapy.Request("http://mirror.bgm.rin.cat/game/list/"+username, callback = self.merge, meta = { 'uid': uid })

        if len(response.xpath(".//*[@id='book']")):
            yield scrapy.Request("http://mirror.bgm.rin.cat/book/list/"+username, callback = self.merge, meta = { 'uid': uid })

        if len(response.xpath(".//*[@id='music']")):
            yield scrapy.Request("http://mirror.bgm.rin.cat/music/list/"+username, callback = self.merge, meta = { 'uid': uid })

        if len(response.xpath(".//*[@id='real']")):
            yield scrapy.Request("http://mirror.bgm.rin.cat/real/list/"+username, callback = self.merge, meta = { 'uid': uid })

    def merge(self, response):
        followlinks = response.xpath(".//div[@id='columnA']/div/div[1]/ul/li[2]//@href").extract() # a list of links
        for link in followlinks:
            yield scrapy.Request(u"http://mirror.bgm.rin.cat"+link, callback = self.parse_recorder, meta = { 'uid': response.meta['uid'] })

    def parse_recorder(self, response):
        state = response.url.split('/')[-1].split('?')[0]
        tp = response.url.split('/')[-4]

        items = response.xpath(".//*[@id='browserItemList']/li")
        for item in items:
            item_id = int(re.match(r"item_(\d+)",item.xpath("./@id").extract()[0]).group(1))
            item_date = parsedate(item.xpath("./div/p[@class='collectInfo']/span[@class='tip_j']/text()").extract()[0])
            if item.xpath("./div/p[@class='collectInfo']/span[@class='tip']"):
                item_tags = item.xpath("./div/p[@class='collectInfo']/span[@class='tip']/text()").extract()[0].split(u' ')[2:-1]
            else:
                item_tags=None

            try_match = re.match(r'sstars(\d+) starsinfo', item.xpath("./div/p[@class='collectInfo']/span[1]/@class").extract()[0])
            if try_match:
                item_rate = try_match.group(1)
                item_rate = int(item_rate)
            else:
                item_rate = None

            comment = item.xpath(".//div[@class='text']/text()").extract()[0] if len(item.xpath(".//div[@class='text']")) > 0 else None

            watchRecord = Record(
                uid = response.meta['uid'],
                typ = tp, state = state,
                iid = item_id,
                adddate = item_date
                )
            if item_tags:
                watchRecord["tags"]=item_tags
            if item_rate:
                watchRecord["rate"]=item_rate
            if comment:
                watchRecord["comment"]=comment.translate(mpa)
            yield watchRecord

        if len(items)==24:
            yield scrapy.Request(getnextpage(response.url),callback = self.parse_recorder, meta = { 'uid': response.meta['uid'] })

class FriendsSpider(scrapy.Spider):
    name='friends'
    handle_httpstatus_list = [302]
    def __init__(self, *args, **kwargs):
        super(FriendsSpider, self).__init__(*args, **kwargs)
        if not hasattr(self, 'id_max'):
            self.id_max=400000
        if not hasattr(self, 'id_min'):
            self.id_min=1
        self.start_urls = ["http://mirror.bgm.rin.cat/user/"+str(i)+"/friends" for i in range(int(self.id_min),int(self.id_max))]

    def parse(self, response):
        user = response.url.split('/')[-2]
        lst = response.xpath(".//*[@id='memberUserList']/li//@href").extract()
        for itm in lst:
            yield Friend(user = user, friend = str(itm.split('/')[-1]))

class SubjectInfoSpider(scrapy.Spider):
    name="subjectinfo"
    def __init__(self, *args, **kwargs):
        super(SubjectInfoSpider, self).__init__(*args, **kwargs)
        if not hasattr(self, 'id_max'):
            self.id_max=300000
        if not hasattr(self, 'id_min'):
            self.id_min=1
        self.start_urls = ["http://mirror.bgm.rin.cat/subject/"+str(i) for i in range(int(self.id_min),int(self.id_max))]

    def parse(self, response):
        subject_id = int(response.url.split('/')[-1])
        if not response.xpath(".//*[@id='headerSubject']"):
            return
        if response.xpath(".//div[@class='tipIntro']"):
            return
        typestring = response.xpath(".//div[@class='global_score']/div/small[1]/text()").extract()[0]
        typestring = typestring.split(' ')[1];

        infobox = [itm.extract()[:-2] for itm in response.xpath(".//div[@class='infobox']//span/text()")]
        infobox = set(infobox)

        relations = [itm.extract() for itm in response.xpath(".//ul[@class='browserCoverMedium clearit']/li[@class='sep']/span/text()")]
        relations = set(relations)

        yield SubjectInfo(subjectid=subject_id,
                          subjecttype=typestring,
                          infobox=infobox,
                          relations=relations)

class SubjectSpider(scrapy.Spider):
    name="subject"
    def __init__(self, *args, **kwargs):
        super(SubjectSpider, self).__init__(*args, **kwargs)
        if hasattr(self, 'itemlist'):
            itemlist = []
            with open(self.itemlist, 'r') as fr:
                while True:
                    l = fr.readline().strip()
                    if not l: break;
                    itemlist.append(l)
            self.start_urls = ["http://mirror.bgm.rin.cat/subject/"+i for i in itemlist]
        else:
            if not hasattr(self, 'id_max'):
                self.id_max=300000
            if not hasattr(self, 'id_min'):
                self.id_min=1
            self.start_urls = ["http://mirror.bgm.rin.cat/subject/"+str(i) for i in range(int(self.id_min),int(self.id_max))]

    def parse(self, response):
        subjectid = int(response.url.split('/')[-1]) # trueid
        if not response.xpath(".//*[@id='headerSubject']"):
            return
        
        # This is used to filter those locked items
        # However, considering that current Bangumi ranking list does not exclude blocked items,
        # we include them in our spider.
        #if response.xpath(".//div[@class='tipIntro']"):
        #    return;

        if 'redirect_urls' in response.meta:
            order = int(response.meta['redirect_urls'][0].split('/')[-1])
        else:
            order = subjectid; # id

        subjectname = response.xpath(".//*[@id='headerSubject']/h1/a/attribute::title").extract()[0]
        if not subjectname:
            subjectname = response.xpath(".//*[@id='headerSubject']/h1/a/text()").extract()[0]

        subjecttype = response.xpath(".//div[@class='global_score']/div/small[1]/text()").extract()[0]
        subjecttype = subjecttype.split(' ')[1].lower();

        infokey = [itm[:-2] for itm in response.xpath(".//div[@class='infobox']//li/span/text()").extract()]
        infoval = response.xpath(".//div[@class='infobox']//li")
        infobox = dict()
        alias = []
        for key,val in zip(infokey, infoval):
            if val.xpath("a"):
                infobox[key]=[ref.split('/')[-1] for ref in
                    val.xpath("a/@href").extract()]
            if key == '别名':
                alias.append(val.xpath('text()').extract()[0])

        relateditms = response.xpath(".//ul[@class='browserCoverMedium clearit']/li")
        relations = dict()
        for itm in relateditms:
            if itm.xpath("@class"):
                relationtype = itm.xpath("span/text()").extract()[0]
                relations[relationtype]=[itm.xpath("a[@class='title']/@href").
                                extract()[0].split('/')[-1]]
            else:
                relations[relationtype].append(itm.xpath("a[@class='title']/@href").
                                      extract()[0].split('/')[-1])
        brouche = response.xpath(".//ul[@class='browserCoverSmall clearit']/li")
        if brouche:
            relations['单行本']=[itm.split('/')[-1] for itm in
                           brouche.xpath("a/@href").extract()]

        yield Subject(subjectid=subjectid,
                      subjecttype=subjecttype,
                      subjectname=subjectname,
                      order=order,
                      alias=alias,
                      staff=infobox,
                      relations=relations)
