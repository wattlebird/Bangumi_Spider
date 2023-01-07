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
    def __init__(self, use_original=True, id_min=1, id_max=800000, *args, **kwargs):
        super(UserSpider, self).__init__(*args, **kwargs)
        self.use_original = use_original if type(use_original) == bool else use_original == "True"
        self.id_min = int(id_min)
        self.id_max = int(id_max)
        self.start_urls = [f"https://{'bgm.tv' if self.use_original else 'mirror.bgm.rincat.ch'}/user/{i}" for i in range(self.id_min,self.id_max)]

    def parse(self, response):
        if len(response.xpath(".//*[@id='headerProfile']"))==0:
            return
        user = response.xpath("/html/body/div[1]/div[3]/div/div[1]/h1/div[2]/small/text()").extract()[0][1:]
        nickname = response.xpath("/html/body/div[1]/div[3]/div/div[1]/h1/div[2]/a/text()").extract()[0].translate(mpa)

        # Is blocked?
        if len(response.xpath("//ul[@class='timeline']/li"))==0:
            return;

        if not 'redirect_urls' in response.meta:
            uid = int(user)
        else:
            uid = int(response.meta['redirect_urls'][0].split('/')[-1])
        date = response.xpath("/html/body/div[1]/div[4]/div[1]/div[1]/div/div[1]/ul/li[1]/span[2]/text()").extract()[0].split(' ')[0]
        date = parsedate(date)

        yield User(name=user, nickname=nickname, uid=uid, joindate=date)

class IndexSpider(scrapy.Spider):
    name='index'
    def __init__(self, use_original=True, id_min=1, id_max=50000, *args, **kwargs):
        super(IndexSpider, self).__init__(*args, **kwargs)
        self.use_original = use_original if type(use_original) == bool else use_original == "True"
        self.id_min = int(id_min)
        self.id_max = int(id_max)
        self.start_urls = [f"https://{'bgm.tv' if self.use_original else 'mirror.bgm.rincat.ch'}/index/{i}" for i in range(int(self.id_min),int(self.id_max))]

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
    def __init__(self, use_original=True, id_min=1, id_max=800000, *args, **kwargs):
        super(RecordSpider, self).__init__(*args, **kwargs)
        self.use_original = use_original if type(use_original) == bool else use_original == "True"
        self.id_min = int(id_min)
        self.id_max = int(id_max)
        self.start_urls = [f"https://{'bgm.tv' if self.use_original else 'mirror.bgm.rincat.ch'}/user/{i}" for i in range(int(self.id_min),int(self.id_max))]

    def parse(self, response):
        username = response.url.split('/')[-1]
        if (not response.xpath(".//*[@id='headerProfile']")) or response.xpath(".//div[@class='tipIntro']"):
            return
        if username in blockusers:
            return
        uid = int(response.meta['redirect_urls'][0].split('/')[-1]) if 'redirect_urls' in response.meta else int(username)
        nickname = next(iter(response.xpath(".//*[@class='headerContainer']//*[@class='inner']/a/text()").extract()), "").translate(mpa)

        date = response.xpath(".//*[@id='user_home']/div[@class='user_box clearit']/ul/li[1]/span[2]/text()").extract()[0].split(' ')[0]
        date = parsedate(date)

        yield User(name=username, nickname=nickname, uid=uid, joindate=date)

        if len(response.xpath(".//*[@id='anime']")):
            yield scrapy.Request(f"https://{'bgm.tv' if self.use_original else 'mirror.bgm.rincat.ch'}/anime/list/{username}", callback = self.merge, meta = { 'uid': uid })

        if len(response.xpath(".//*[@id='game']")):
            yield scrapy.Request(f"https://{'bgm.tv' if self.use_original else 'mirror.bgm.rincat.ch'}/game/list/{username}", callback = self.merge, meta = { 'uid': uid })

        if len(response.xpath(".//*[@id='book']")):
            yield scrapy.Request(f"https://{'bgm.tv' if self.use_original else 'mirror.bgm.rincat.ch'}/book/list/{username}", callback = self.merge, meta = { 'uid': uid })

        if len(response.xpath(".//*[@id='music']")):
            yield scrapy.Request(f"https://{'bgm.tv' if self.use_original else 'mirror.bgm.rincat.ch'}/music/list/{username}", callback = self.merge, meta = { 'uid': uid })

        if len(response.xpath(".//*[@id='real']")):
            yield scrapy.Request(f"https://{'bgm.tv' if self.use_original else 'mirror.bgm.rincat.ch'}/real/list/{username}", callback = self.merge, meta = { 'uid': uid })

    def merge(self, response):
        followlinks = response.xpath("//ul[@class='navSubTabs']/li/a/@href").extract() # a list of links
        for link in followlinks:
            yield scrapy.Request(f"https://{'bgm.tv' if self.use_original else 'mirror.bgm.rincat.ch'}{link}", callback = self.parse_recorder, meta = { 'uid': response.meta['uid'] })

    def parse_recorder(self, response):
        state = response.url.split('/')[-1].split('?')[0]
        page = 1 if '=' not in response.url else int(response.url.split('=')[1])
        tp = response.url.split('/')[-4]

        items = response.xpath(".//*[@id='browserItemList']/li")
        for item in items:
            item_id = int(re.match(r"item_(\d+)",item.xpath("./@id").extract()[0]).group(1))
            item_date = parsedate(item.xpath("./div/p[@class='collectInfo']/span[@class='tip_j']/text()").extract()[0])
            if item.xpath("./div/p[@class='collectInfo']/span[@class='tip']"):
                item_tags = item.xpath("./div/p[@class='collectInfo']/span[@class='tip']/text()").extract()[0].split(u' ')[2:-1]
            else:
                item_tags=None

            try_match = next(iter(item.xpath("./div/p[@class='collectInfo']/span[@class='starstop-s']/span/@class").extract()), None)
            if try_match is not None:
                mtch = re.match(r'starlight stars(\d+)', try_match)
                item_rate = mtch.group(1)
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

        total_count = int(re.search(r"(\d+)", response.xpath("//ul[@class='navSubTabs']/li/a[@class='focus']/span/text()").extract()[0]).group(1))
        if 24 * page < total_count:
            yield scrapy.Request(getnextpage(response.url),callback = self.parse_recorder, meta = { 'uid': response.meta['uid'] })

class FriendsSpider(scrapy.Spider):
    name='friends'
    handle_httpstatus_list = [302]
    def __init__(self, use_original=True, id_min=1, id_max=800000, *args, **kwargs):
        super(FriendsSpider, self).__init__(*args, **kwargs)
        self.use_original = use_original if type(use_original) == bool else use_original == "True"
        self.id_min = int(id_min)
        self.id_max = int(id_max)
        self.start_urls = [f"https://{'bgm.tv' if self.use_original else 'mirror.bgm.rincat.ch'}/user/{i}/friends" for i in range(int(self.id_min),int(self.id_max))]

    def parse(self, response):
        user = response.url.split('/')[-2]
        lst = response.xpath(".//*[@id='memberUserList']/li//@href").extract()
        for itm in lst:
            yield Friend(user = user, friend = str(itm.split('/')[-1]))

class SubjectSpider(scrapy.Spider):
    name="subject"
    def __init__(self, use_original=True, id_min=1, id_max=450000, *args, **kwargs):
        super(SubjectSpider, self).__init__(*args, **kwargs)
        self.use_original = use_original if type(use_original) == bool else use_original == "True"
        self.id_min = int(id_min)
        self.id_max = int(id_max)
        self.start_urls = [f"https://{'bgm.tv' if self.use_original else 'mirror.bgm.rincat.ch'}/subject/{i}" for i in range(int(self.id_min),int(self.id_max))]

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
