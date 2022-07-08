import os
import datetime

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

# Main Scrapy Configs
BOT_NAME = 'instagram'
SPIDER_MODULES = ['ef_instagram_scraper.spiders']
NEWSPIDER_MODULE = 'ef_instagram_scraper.spiders'
CONCURRENT_REQUESTS = 1

## S3 Filepath and Export Feed

year_month = datetime.datetime.today().strftime('%Y_%m')

s3_filepath ='s3://euclidsfund-data-pipeline/data_acquisition/instagram/raw/%(name)s'
s3_filename ='v1.5.0_'+'%(time)s.json'

FEED_URI = +f'{s3_filepath}/{year_month}/{s3_filename}'
FEED_FORMAT = 'json'

ZYTE_SCHEDULE_START_DAY = 7
RAPIDAPI_DAILY_REQ_LIMIT = 6000