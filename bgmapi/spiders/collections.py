# -*- coding: utf-8 -*-
import scrapy
import json

class CollectionsSpider(scrapy.Spider):
    name = 'collections-api'

    def __init__(self, use_original=True, id_min=1, id_max=800000, *args, **kwargs):
        super(CollectionsSpider, self).__init__(*args, **kwargs)
        self.use_original = use_original if type(use_original) == bool else use_original == "True"
        self.id_min = int(id_min)
        self.id_max = int(id_max)

    def start_requests(self):
        for i in range(self.id_min, self.id_max):
            url = f"https://{'api.bgm.tv' if self.use_original else 'mirror.api.bgm.rincat.ch'}/user/{i}"
            yield scrapy.Request(url)

    def parse(self, res):
        user = res.json()
        if user['usergroup'] == 4 or user['usergroup'] == 5:
            return
        yield scrapy.Request(f"https://{'api.bgm.tv' if self.use_original else 'mirror.api.bgm.rincat.ch'}/v0/users/{user['username']}/collections?limit=50&offset=0",
            callback = self.collection_parser,
            cb_kwargs = { 'offset': 0, 'username': user['username'], 'order': user['id'] })

    def collection_parser(self, res, offset, username, order):
        collection = res.json()
        for item in collection['data']:
            item.pop('subject')
            item['user_id'] = username
            item['user_order'] = order
            yield item
        if offset + len(collection['data']) < collection['total']:
            yield scrapy.Request(f"https://{'api.bgm.tv' if self.use_original else 'mirror.api.bgm.rincat.ch'}/v0/users/{username}/collections?limit=50&offset={offset+50}",
                callback = self.collection_parser,
                cb_kwargs = { 'offset': offset+50, 'username': username, 'order': order })