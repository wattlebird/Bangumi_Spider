# -*- coding: utf-8 -*-
import scrapy
import re
import json
from bgmapi.items import Subject

subjectTypeLut = {
    1: 'book',
    2: 'anime',
    3: 'music',
    4: 'game',
    6: 'real'
}
mpa = dict([(i, None) for i in range(32)])

class SubjectSpider(scrapy.Spider):
    name = 'subject'
    allowed_domains = ['mirror.api.bgm.rin.cat']

    def __init__(self, id_min=1, id_max=300000, *args, **kwargs):
        super(SubjectSpider, self).__init__(*args, **kwargs)
        self.start_urls = ["http://mirror.api.bgm.rin.cat/subject/{0}?responseGroup=medium".format(i) for i in range(int(id_min),int(id_max))]

    def parse(self, response):
        id = re.search(r"(\d+)", response.url).group(1)
        order = id if not "redirect_urls" in response.meta else re.search(r"(\d+)", response.meta["redirect_urls"][0]).group(1)
        
        data = json.loads(response.body_as_unicode())
        if 'error' in data:
            return
        typ = subjectTypeLut[data['type']]
        name = data['name']
        name_cn = data['name_cn']
        rank = data.get('rank', '')
        votenum = data['rating']['total'] if 'rating' in data else 0
        if 'collection' not in data:
            favnum = [0,0,0,0,0]
        else:
            favnum = [
                data['collection'].get('wish', 0),
                data['collection'].get('collect', 0),
                data['collection'].get('doing', 0),
                data['collection'].get('on_hold', 0),
                data['collection'].get('dropped', 0)
            ]
        date = data['air_date']

        staff = dict()
        if data['staff']:
            for stf in data['staff']:
                for role in stf['jobs']:
                    t = staff.setdefault(role, [])
                    t.append(str(stf['id']))
        
        yield Subject(subjectid=id,
                      subjecttype=typ,
                      subjectname=name.translate(mpa),
                      subjectname_cn=name_cn.translate(mpa),
                      order=order,
                      rank=rank,
                      votenum=votenum,
                      favnum=';'.join([str(itm) for itm in favnum]),
                      date=date,
                      staff=staff)