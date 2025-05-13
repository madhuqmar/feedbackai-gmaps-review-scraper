#!/usr/bin/env python
# -*- coding: utf-8 -*-
import boto3
import json
import os
from datetime import datetime
import argparse
import logging
import sys
import csv
import io
import pandas as pd
from termcolor import colored

from googlemaps import GoogleMapsScraper  # make sure this is importable

BUCKET_NAME = 'naturals-reviews'
HEADER = ['id_review', 'caption', 'relative_date', 'review_date', 'retrieval_date', 'rating', 'username', 'n_review_user', 'place_id']

class MonitorS3:

    def __init__(self, url_file, max_reviews):
        with open(url_file, 'r') as furl:
            self.urls = [u.strip() for u in furl]

        self.max_reviews = max_reviews
        self.logger = self.__get_logger()
        self.s3 = boto3.client('s3')

    def scrape_and_monitor_reviews(self):
        with GoogleMapsScraper() as scraper:
            for url in self.urls:
                slug = self.get_slug_from_url(url)
                s3_key = "combined/all_4_naturals_reviews.csv"

                try:
                    # Sort reviews by newest
                    error = scraper.sort_by(url, 1)
                    if error != 0:
                        self.logger.warning(f"⚠️ Sorting failed for {url}")
                        continue

                    offset = 0
                    local_reviews = []

                    while len(local_reviews) < self.max_reviews:
                        self.logger.info(f"Fetching reviews from offset {offset} for {slug}")
                        reviews = scraper.get_reviews(offset, url)
                        if not reviews:
                            break

                        for r in reviews:
                            if len(local_reviews) >= self.max_reviews:
                                break
                            r['retrieval_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            local_reviews.append(r)

                        offset += len(reviews)

                    # Compare with existing data in S3
                    previous_reviews = self.load_s3_reviews(s3_key)
                    previous_ids = set(previous_reviews['id_review']) if not previous_reviews.empty else set()
                    new_df = pd.DataFrame(local_reviews)
                    new_ids = set(new_df['id_review'])

                    diff_ids = new_ids - previous_ids

                    if diff_ids:
                        updated_df = pd.concat([previous_reviews, new_df[new_df['id_review'].isin(diff_ids)]], ignore_index=True)
                        self.upload_csv_to_s3(updated_df, HEADER, s3_key)
                        self.logger.info(f"✅ {len(diff_ids)} new reviews uploaded to all_4_naturals_reviews.csv")
                    else:
                        self.logger.info("No new reviews detected for all_4_naturals_reviews.csv")

                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    self.logger.error(f"{url}: {exc_type}, {fname}, {exc_tb.tb_lineno}")

    def get_slug_from_url(self, url):
        try:
            return url.strip().split('/')[4]
        except IndexError:
            return "place-" + datetime.today().strftime('%Y%m%d%H%M%S')

    def load_s3_reviews(self, key):
        try:
            obj = self.s3.get_object(Bucket=BUCKET_NAME, Key=key)
            df = pd.read_csv(obj['Body'])
            return df
        except Exception as e:
            self.logger.warning(f"No existing file at {key}: {e}")
            return pd.DataFrame(columns=['id_review'])

    def upload_csv_to_s3(self, df, headers, s3_key):
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(headers)
        for _, r in df.iterrows():
            writer.writerow([r.get(k, "") for k in headers])
        self.s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue())
        print(colored(f"✅ Uploaded CSV to s3://{BUCKET_NAME}/{s3_key}", "green"))

    def __get_logger(self):
        logger = logging.getLogger('monitor_s3')
        logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler('monitor_s3.log')
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Monitor Google Maps reviews and store in S3')
    parser.add_argument('--i', type=str, default='urls.txt', help='target URLs file')
    parser.add_argument('--N', type=int, default=100, help='Max number of reviews per place')
    args = parser.parse_args()

    monitor = MonitorS3(args.i, args.N)
    try:
        monitor.scrape_and_monitor_reviews()
    except Exception as e:
        monitor.logger.error(f'Unhandled error: {e}')
