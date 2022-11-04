# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy import signals
import datetime
import os
import glob
from .settings import *
if UPLOAD_TO_AZURE_STORAGE:
    from azure.storage.blob import BlobServiceClient

class AzureBlobPipeline(object):
    def __init__(self):
        self.files = dict()

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        return pipeline

    def open_spider(self, spider):
        if spider.name=='subject-api':
            lst = glob.glob('subject-api*.jsonlines')
        if spider.name=='collections-api':
            lst = glob.glob('collections-api*.jsonlines')
        if spider.name=='user-api':
            lst = glob.glob('user-api*.jsonlines')
        print(f"Existing files removed: {lst}")
        for f in lst:
            os.remove(f)

    def spider_closed(self, spider):
        print("spider_closed invoked")
        if spider.name=='subject-api':
            newname = f"subject-api.{datetime.datetime.utcnow().strftime('%Y-%m-%d')}.jsonlines"
            os.rename("subject-api.jsonlines", newname)
        if spider.name=='collections-api':
            newname = f"collections-api.{datetime.datetime.utcnow().strftime('%Y-%m-%d')}.jsonlines"
            os.rename("collections-api.jsonlines", newname)
        if spider.name=='user-api':
            newname = f"user-api.{datetime.datetime.utcnow().strftime('%Y-%m-%d')}.jsonlines"
            os.rename("user-api.jsonlines", newname)
        if UPLOAD_TO_AZURE_STORAGE:
            blobServiceClient = BlobServiceClient.from_connection_string(AZURE_ACCOUNT_KEY)
            blobClient = blobServiceClient.get_blob_client(container=AZURE_CONTAINER, blob=newname)

            with open(newname, "rb") as data:
                blobClient.upload_blob(data)
