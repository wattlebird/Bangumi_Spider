# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy import signals
import datetime
import os
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

    def spider_closed(self, spider):
        print("spider_closed involked")
        if spider.name=='subject-api':
            os.rename("subject-api.jsonlines", f"subject-api.{datetime.datetime.utcnow().strftime('%Y-%m-%d')}.jsonlines")
        if spider.name=='collections-api':
            os.rename("collections-api.jsonlines", f"collections-api.{datetime.datetime.utcnow().strftime('%Y-%m-%d')}.jsonlines")
        if spider.name=='user-api':
            os.rename("user-api.jsonlines", f"user-api.{datetime.datetime.utcnow().strftime('%Y-%m-%d')}.jsonlines")
        if UPLOAD_TO_AZURE_STORAGE:
            blobServiceClient = BlobServiceClient.from_connection_string(AZURE_ACCOUNT_KEY)
            blobClient = blobServiceClient.get_blob_client(container=AZURE_CONTAINER, blob=newname)

            with open(newname, "rb") as data:
                blobClient.upload_blob(data)
