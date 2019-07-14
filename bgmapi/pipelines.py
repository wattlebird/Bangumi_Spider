# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy import log
from scrapy import signals
from scrapy.exporters import CsvItemExporter
import datetime
import os
from .settings import *
if UPLOAD_TO_AZURE_STORAGE:
    from azure.storage.blob import BlockBlobService, ContentSettings

class TsvPipeline(object):
    def __init__(self):
        self.files = dict()

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def spider_opened(self, spider):
        file = open(spider.name+'-'+datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")+'.tsv*', 
                           'wb')
        self.files[spider] = file

        self.exporter = CsvItemExporter(file, include_headers_line=True, join_multivalued=';', encoding="utf-8", delimiter='\t')
        if spider.name=='user-api':
            self.exporter.fields_to_export = ['uid', 'name', 'nickname', 'group']
        elif spider.name=='subject-api':
            self.exporter.fields_to_export = ['subjectid', 'order', 'subjectname', 'subjectname_cn', 'subjecttype', 'rank', 'date', 'votenum', 'favnum', 'staff']
        
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        file = self.files.pop(spider)
        filename = file.name
        newname = filename[:-5]+'-'+datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")+'.tsv'
        file.close()
        os.rename(filename, newname)
        if UPLOAD_TO_AZURE_STORAGE:
            block_blob_service = BlockBlobService(account_name=AZURE_ACCOUNT_NAME, account_key=AZURE_ACCOUNT_KEY)
            block_blob_service.create_blob_from_path(AZURE_CONTAINER,
                                                    newname,
                                                    newname,
                                                    content_settings=ContentSettings(content_type='text/tab-separated-values')
                                                            )
                                                            

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item