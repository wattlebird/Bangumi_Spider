# This package will contain the spiders of your Scrapy project
#
# Please refer to the documentation for information on how to create and manage
# your spiders.
from .subject import SubjectSpider
from .user import UserSpider

__all__ = ['SubjectSpider', 'UserSpider']