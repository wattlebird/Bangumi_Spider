# -*- coding: utf-8 -*-
import scrapy
import json
from bgmapi.items import User


class UserSpider(scrapy.Spider):
    name = 'user-api'
    allowed_domains = ['http://mirror.api.bgm.rincat.ch/user']

    def __init__(self, id_min=1, id_max=300000, *args, **kwargs):
        super(UserSpider, self).__init__(*args, **kwargs)
        self.start_urls = ["http://mirror.api.bgm.rincat.ch/user/{0}".format(i) for i in range(int(id_min),int(id_max))]

    def parse(self, response):
        data = json.loads(response.body_as_unicode())
        if 'error' in data:
            return
        yield User(
            name=data['username'],
            nickname=data['nickname'],
            uid=data['id'],
            group=data['usergroup']
        )