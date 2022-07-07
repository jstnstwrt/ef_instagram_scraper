# account_basics.py

import scrapy
import pandas as pd
import json

# for accessing files on AWS S3
import os
import boto3

class AccountBasics(scrapy.Spider):

	name = 'account_basics'

	def build_request(self,username=None):


		# Build endpoint URL to grab account basics.
		base_url = 'https://www.instagram.com/'
		params = '/?__a=1'
		endpoint = base_url + str(username) + params

		# Add headers
		headers = {
		  'authority': 'www.instagram.com',
		  'cache-control': 'max-age=0',
		  'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
		  'sec-ch-ua-mobile': '?0',
		  'sec-ch-ua-platform': '"macOS"',
		  'upgrade-insecure-requests': '1',
		  'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
		  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
		  'sec-fetch-site': 'none',
		  'sec-fetch-mode': 'navigate',
		  'sec-fetch-user': '?1',
		  'sec-fetch-dest': 'document',
		  'accept-language': 'en-US,en;q=0.9',
		  'cookie': 'csrftoken=AHwZliXQ3OFBisKtj0vH314XR3wG2kTx; mid=YcvX7AAEAAFyFPaAwRLQxKXque8m; ig_did=9E06B2B4-45F9-4397-B04A-4B9DB5C6FEB7; ig_nrcb=1; csrftoken=LyvNn2uDUx6Oo4tP37QkDMeaD7gMtn8w; ds_user_id=332324252; rur="NAO\\054332324252\\0541672272530:01f79ab1f38fd1e536e502764b485c5188c8af4be95a6b8ecdf2eb40b786345a5bda8f75"'
		}

		# Compose request
		request = scrapy.Request(
			url=endpoint,
			callback=self.parse,
			method='GET',
			headers=headers,
		)

		return request


	def start_requests(self):

		# connect to aws s3
		AWS_ACCESS_KEY_ID = self.settings['AWS_ACCESS_KEY_ID']
		AWS_SECRET_ACCESS_KEY = self.settings['AWS_SECRET_ACCESS_KEY']

		print(f'  __________________________________________                            {AWS_SECRET_ACCESS_KEY}')

		s3 = boto3.client(
		    's3',
		    aws_access_key_id=AWS_ACCESS_KEY_ID,
		    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
		)

		match_algo_version = 'v1.0.0'
		bucket = 'euclidsfund-data-pipeline'
		prefix = f'/pipeline/artist_matches/artist_matches_{match_algo_version}_'

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

		user_list = list(df.instagram_username.values)

		# iterate and yield reqs for all artists
		for i, username in enumerate(user_list):
			print('{}-{}-{}'.format(i,len(user_list),username))
			yield self.build_request(username)


	def parse(self,response):

		d = json.loads(response.text)

		basics_data = d['graphql']['user']

		yield basics_data




