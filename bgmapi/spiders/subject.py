# -*- coding: utf-8 -*-
import scrapy
import re
import json

class SubjectSpider(scrapy.Spider):
    name = 'subject-api'

    def __init__(self, use_original=True, token="", id_min=1, id_max=400000, *args, **kwargs):
        super(SubjectSpider, self).__init__(*args, **kwargs)
        self.token = token
        self.use_original = use_original if type(use_original) == bool else use_original == "True"
        self.id_min = int(id_min)
        self.id_max = int(id_max)

    def start_requests(self):
        for i in range(self.id_min, self.id_max):
            url = "https://{0}/v0/subjects/{1}".format("api.bgm.tv" if self.use_original else "mirror.api.bgm.rincat.ch", i)
            yield scrapy.Request(url, headers={'Authorization': 'Bearer {0}'.format(self.token)})


    def parse(self, response):
        if response.status == 404:
            return
        id = re.search(r"(\d+)", response.url).group(1)
        order = id if not "redirect_urls" in response.meta else response.meta['redirect_urls'][0].split('/')[-1]
        
        data = response.json()
        if 'error' in data:
            return
        if data['locked']:
            return
        data.pop('images')
        data.pop('platform')
        data.pop('summary')
        data.pop('infobox')
        data.pop('total_episodes')
        data.pop('eps')
        data.pop('volumes')
        data.pop('locked')
        data.pop('nsfw')
        data['order'] = int(order)
        yield data
