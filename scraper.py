# -*- coding: utf-8 -*-
from googlemaps import GoogleMapsScraper
from datetime import datetime
import argparse
import boto3
import json
from termcolor import colored

ind = {'most_relevant': 0, 'newest': 1, 'highest_rating': 2, 'lowest_rating': 3}
BUCKET_NAME = 'naturals-reviews'

def upload_to_s3(reviews, place_slug):
    s3 = boto3.client('s3')
    today = datetime.today().strftime('%Y-%m-%d')
    key = f"{place_slug}/{today}.json"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(reviews, indent=2, default=str)
    )
    print(colored(f"✅ Uploaded {len(reviews)} reviews to s3://{BUCKET_NAME}/{key}", "green"))

def get_slug_from_url(url):
    # Extract slug or fallback to timestamp-based folder
    try:
        return url.strip().split('/')[4]
    except IndexError:
        return "place-" + datetime.today().strftime('%Y%m%d%H%M%S')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Google Maps reviews scraper.')
    parser.add_argument('--N', type=int, default=100, help='Number of reviews to scrape')
    parser.add_argument('--i', type=str, default='urls.txt', help='target URLs file')
    parser.add_argument('--sort_by', type=str, default='newest', help='most_relevant, newest, highest_rating or lowest_rating')
    parser.add_argument('--place', dest='place', action='store_true', help='Scrape place metadata')
    parser.add_argument('--debug', dest='debug', action='store_true', help='Run scraper using browser graphical interface')
    parser.add_argument('--source', dest='source', action='store_true', help='Add source url to review data')
    parser.set_defaults(place=False, debug=False, source=False)

    args = parser.parse_args()

    with GoogleMapsScraper(debug=args.debug) as scraper:
        with open(args.i, 'r') as urls_file:
            for url in urls_file:
                url = url.strip()
                slug = get_slug_from_url(url)

                if args.place:
                    print(scraper.get_account(url))
                else:
                    error = scraper.sort_by(url, ind[args.sort_by])
                    all_reviews = []

                    if error == 0:
                        n = 0
                        while n < args.N:
                            print(colored(f'[Fetching from offset {n}]', 'cyan'))

                            reviews = scraper.get_reviews(n)
                            if len(reviews) == 0:
                                break

                            for r in reviews:
                                if args.source:
                                    r['source_url'] = url
                                all_reviews.append(r)

                            n += len(reviews)

                        upload_to_s3(all_reviews, slug)
                    else:
                        print(colored(f'⚠️  Failed to sort reviews for {url}', 'red'))
