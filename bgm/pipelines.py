# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.exceptions import DropItem
from twisted.enterprise import adbapi
from scrapy import signals
from scrapy.exporters import JsonLinesItemExporter, CsvItemExporter
import pickle
import codecs
import datetime
import os
from .settings import *
if UPLOAD_TO_AZURE_STORAGE:
    from azure.storage.blob import BlobServiceClient


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
        file = open(spider.name+'-'+datetime.datetime.utcnow().strftime('%Y-%m-%d')+'.tsv*', 'wb')
        self.files[spider] = [file]
        self.exporter = CsvItemExporter(file, include_headers_line=True, join_multivalued=';', encoding="utf-8", delimiter='\t')
        if spider.name=='record':
            userfile = open('user-'+datetime.datetime.utcnow().strftime("%Y-%m-%d")+'.tsv*', 'wb')
            self.files[spider].append(userfile)
            self.userexporter = CsvItemExporter(userfile, include_headers_line=True, join_multivalued=';', encoding="utf-8", delimiter='\t')
            self.userexporter.fields_to_export = ['uid', 'name', 'nickname', 'joindate']
            self.exporter.fields_to_export = ['uid', 'iid', 'typ', 'state', 'adddate', 'rate', 'tags', 'comment']
        elif spider.name=='user':
            self.exporter.fields_to_export = ['uid', 'name', 'nickname', 'joindate']
        elif spider.name=='subject':
            self.exporter.fields_to_export = ['subjectid', 'subjecttype', 'subjectname', 'order', 'alias', 'staff', 'relations']
        elif spider.name=='index':
            self.exporter.fields_to_export = ['indexid', 'creator', 'favourite', 'date', 'items']
        elif spider.name=='friends':
            self.exporter.fields_to_export = ['user', 'friend']

        self.exporter.start_exporting()
        if spider.name=='record':
            self.userexporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        file = self.files[spider].pop(0)
        filename = file.name
        newname = filename[:-5]+'-'+datetime.datetime.utcnow().strftime('%Y-%m-%d')+'.tsv'
        file.close()
        os.rename(filename, newname)
        if spider.name == 'record':
            self.userexporter.finish_exporting()
            file = self.files[spider].pop(0)
            userfilename = file.name
            newuserfilename = userfilename[:-5]+'-'+datetime.datetime.utcnow().strftime("%Y-%m-%d")+'.tsv'
            file.close()
            os.rename(userfilename, newuserfilename)

        if UPLOAD_TO_AZURE_STORAGE:
            blobServiceClient = BlobServiceClient.from_connection_string(AZURE_ACCOUNT_KEY)
            blobClient = blobServiceClient.get_blob_client(container=AZURE_CONTAINER, blob=newname)
            with open(newname, "rb") as data:
                blobClient.upload_blob(data)

            if spider.name == 'record':
                blobClient = blobServiceClient.get_blob_client(container=AZURE_CONTAINER, blob=newuserfilename)
                with open(newuserfilename, "rb") as data:
                    blobClient.upload_blob(data)

    def process_item(self, item, spider):
        if spider.name == 'record':
            if 'joindate' in item:
                self.userexporter.export_item(item)
            else:
                self.exporter.export_item(item)
        else:
            self.exporter.export_item(item)
        return item