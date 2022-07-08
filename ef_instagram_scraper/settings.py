import os

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

# Main Scrapy Configs
BOT_NAME = 'instagram'
SPIDER_MODULES = ['ef_instagram_scraper.spiders']
NEWSPIDER_MODULE = 'ef_instagram_scraper.spiders'
CONCURRENT_REQUESTS = 1

## S3 Filepath and Export Feed
FEED_URI = 's3://euclidsfund-data-pipeline/data_acquisition/instagram/raw/%(name)s/v1.5.0_%(time)s.json'
FEED_FORMAT = 'json'
