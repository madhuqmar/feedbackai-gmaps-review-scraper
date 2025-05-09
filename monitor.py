#!/usr/bin/env python
# -*- coding: utf-8 -*-
import boto3
import json
import os
from datetime import datetime, timedelta
import argparse
import logging
import sys

from googlemaps import GoogleMapsScraper  # make sure this is importable

BUCKET_NAME = 'naturals-reviews'

class MonitorS3:

    def __init__(self, url_file, from_date):
        with open(url_file, 'r') as furl:
            self.urls = [u.strip() for u in furl]

        self.min_date_review = datetime.strptime(from_date, '%Y-%m-%d')
        self.logger = self.__get_logger()
        self.s3 = boto3.client('s3')

    def scrape_gm_reviews(self):
        with GoogleMapsScraper() as scraper:
            for url in self.urls:
                try:
                    error = scraper.sort_by_date(url)
                    if error == 0:
                        stop = False
                        offset = 0
                        all_reviews = []
                        while not stop:
                            rlist = scraper.get_reviews(offset)
                            for r in rlist:
                                r['timestamp'] = self.__parse_relative_date(r['relative_date'])
                                stop = self.__stop(r)
                                if not stop:
                                    all_reviews.append(r)
                                else:
                                    break
                            offset += len(rlist)

                        if all_reviews:
                            self.save_to_s3(url, all_reviews)
                            self.logger.info(f'{url} : {len(all_reviews)} new reviews')
                        else:
                            self.logger.info(f'{url} : No new reviews')
                    else:
                        self.logger.warning(f'Sorting failed for {url}')
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    self.logger.error(f'{url}: {exc_type}, {fname}, {exc_tb.tb_lineno}')

    def __stop(self, r):
        return r['timestamp'] < self.min_date_review

    def save_to_s3(self, url, reviews):
        today = datetime.today().strftime('%Y-%m-%d')
        slug = url.split('/')[4] if '/' in url else 'place'
        key = f"{slug}/{today}.json"
        self.s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=json.dumps(reviews, indent=2, default=str))

    def __parse_relative_date(self, string_date):
        curr_date = datetime.now()
        split_date = string_date.split(' ')
        n = split_date[0]
        delta = split_date[1]
        return curr_date - {
            'year': timedelta(days=365),
            'years': timedelta(days=365 * int(n)),
            'month': timedelta(days=30),
            'months': timedelta(days=30 * int(n)),
            'week': timedelta(weeks=1),
            'weeks': timedelta(weeks=int(n)),
            'day': timedelta(days=1),
            'days': timedelta(days=int(n)),
            'hour': timedelta(hours=1),
            'hours': timedelta(hours=int(n)),
            'minute': timedelta(minutes=1),
            'minutes': timedelta(minutes=int(n)),
            'moments': timedelta(seconds=1)
        }.get(delta, timedelta())

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
    parser.add_argument('--from-date', type=str, required=True, help='Start date in format: YYYY-MM-DD')
    args = parser.parse_args()

    monitor = MonitorS3(args.i, args.from_date)
    try:
        monitor.scrape_gm_reviews()
    except Exception as e:
        monitor.logger.error(f'Unhandled error: {e}')
