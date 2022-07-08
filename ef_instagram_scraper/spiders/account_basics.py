# account_basics.py

import scrapy
import pandas as pd
import json
import datetime

# for accessing files on AWS S3
import os
import boto3

class AccountBasics(scrapy.Spider):

	name = 'account_basics'

	def build_request(self,pk=None):

		# Add API key
		RAPIDAPIKEY = self.settings['RAPIDAPIKEY']

		# Build endpoint URL to grab account basics.
		url = "https://instagram47.p.rapidapi.com/email_and_details"
		endpoint = f'{url}?userid={pk}'

		# Add headers & query params
		headers = {
		    "X-RapidAPI-Key": RAPIDAPIKEY,
		    "X-RapidAPI-Host": "instagram47.p.rapidapi.com"
		}

		# Compose request
		request = scrapy.Request(
			url=endpoint,
			callback=self.parse,
			method='GET',
			headers=headers
		)

		return request


	def start_requests(self):

		###########################################
		### CONNECTING TO S3 and IMPORTING DATA ###
		###########################################

		AWS_ACCESS_KEY_ID = self.settings['AWS_ACCESS_KEY_ID']
		AWS_SECRET_ACCESS_KEY = self.settings['AWS_SECRET_ACCESS_KEY']

		s3 = boto3.client(
		    's3',
		    aws_access_key_id=AWS_ACCESS_KEY_ID,
		    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
		)

		match_algo_version = 'v1.0.0'
		bucket = 'euclidsfund-data-pipeline'
		prefix = f'pipeline/artist_matches/artist_matches_{match_algo_version}_'

		# identify the latest export of preprocessed artist slugs
		list_of_files = []
		results = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
		for obj in results['Contents']:
		    
		    res_key = obj['Key']
		    if res_key != prefix:
		        list_of_files.append(res_key)
		        
		latest_fp = list_of_files[-1]

		# pull down data from identified (bucket,key) pair
		s3_file_contents = s3.get_object(Bucket=bucket, Key=latest_fp) 
		df = pd.read_csv(s3_file_contents['Body'])


		###########################################
		### Batching Data for Day's Scrape
		###########################################

		# Note: Zyte start day corresponds to periodic schedule, ie., 7th day of each month
		# Required because of RapidAPIs 8K/reqs per day rate limit

		ZYTE_SCHEDULE_START_DAY = int(self.settings['ZYTE_SCHEDULE_START_DAY'])
		RAPIDAPI_DAILY_REQ_LIMIT = int(self.settings['RAPIDAPI_DAILY_REQ_LIMIT'])

		# How many days after start day are we today?
		# Ie., how many prior batches have we completed?
		today = datetime.datetime.today().day
		batch_index = today - ZYTE_SCHEDULE_START_DAY

		assert batch_index >= 0 , "Batch index should not be negative."

		# Defining the next batch
		start_batch = batch_index * RAPIDAPI_DAILY_REQ_LIMIT
		end_batch = (batch_index +1) * RAPIDAPI_DAILY_REQ_LIMIT

		batch_df = df.iloc[start_batch:end_batch]

		# Error out if there are no more batches avail
		assert batch_df.shape[0] > 0 , "No matched artists to scrape."

		user_list = list(batch_df.sample(5).pk.values)

		# iterate and yield reqs for all artists
		for i, pk in enumerate(user_list):
			print('{}-{}-{}'.format(i,len(user_list),pk))
			yield self.build_request(pk)


	def parse(self,response):

		basics_data = json.loads(response.text)

		yield basics_data




