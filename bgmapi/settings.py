# -*- coding: utf-8 -*-

# Scrapy settings for bgmapi project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'bgmapi'

SPIDER_MODULES = ['bgmapi.spiders']
NEWSPIDER_MODULE = 'bgmapi.spiders'


# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'bgmapi (https://github.com/wattlebird/Bangumi_Spider)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False
LOG_LEVEL='INFO'

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
DEFAULT_REQUEST_HEADERS = {
   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
   'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64…) Gecko/20100101 Firefox/63.0'
}

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
SPIDER_MIDDLEWARES = {
#    'scrapy.spidermiddlewares.httperror.HttpErrorMiddleware': 404,
}

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    'bgmapi.middlewares.BgmapiDownloaderMiddleware': 543,
#}

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
#}

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    'bgmapi.pipelines.AzureBlobPipeline': 300,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
AUTOTHROTTLE_TARGET_CONCURRENCY = 0.25
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False
RETRY_TIMES = 10
# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = 'httpcache'
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

DUPEFILTER_CLASS = 'scrapy.dupefilters.BaseDupeFilter'

FEEDS = {
    '%(name)s.jsonlines': {
        'format': 'jsonlines',
        'overwrite': True
    }
}
FEED_EXPORT_ENCODING = 'utf-8'

########################################
# The following settings are only applicable to Ronnie Wang's spider setting
# Because Ronnie only uses Azure for storage.
UPLOAD_TO_AZURE_STORAGE = False
AZURE_ACCOUNT_NAME = "ikely"  # this is the subdomain to https://*.blob.core.windows.net/
AZURE_ACCOUNT_KEY = ""
AZURE_CONTAINER = 'bangumi'   # the name of the container (you should have already created it)
########################################