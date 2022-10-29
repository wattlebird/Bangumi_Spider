# -*- coding: utf-8 -*-
import scrapy
import json


class UserSpider(scrapy.Spider):
    name = 'user-api'

    def __init__(self, use_original=True, id_min=1, id_max=800000, *args, **kwargs):
        super(UserSpider, self).__init__(*args, **kwargs)
        self.use_original = use_original if type(use_original) == bool else use_original == "True"
        self.id_min = int(id_min)
        self.id_max = int(id_max)

    def start_requests(self):
        for i in range(self.id_min, self.id_max):
            url = f"https://{'api.bgm.tv' if self.use_original else 'mirror.api.bgm.rincat.ch'}/user/{i}"
            yield scrapy.Request(url)

    def parse(self, res):
        user = res.json()
        user.pop('avatar')
        yield user