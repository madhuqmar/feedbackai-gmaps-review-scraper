# -*- coding: utf-8 -*-
from googlemaps import GoogleMapsScraper
from datetime import datetime
import argparse
import boto3
import csv
import io
from termcolor import colored

ind = {'most_relevant': 0, 'newest': 1, 'highest_rating': 2, 'lowest_rating': 3}
BUCKET_NAME = 'naturals-reviews'

HEADER = ['id_review', 'caption', 'relative_date', 'review_date', 'retrieval_date', 'rating', 'username', 'n_review_user', 'place_id']
HEADER_W_SOURCE = ['id_review', 'caption', 'relative_date','retrieval_date', 'rating', 'username', 'n_review_user', 'n_photo_user', 'url_user', 'url_source']

def upload_csv_to_s3(reviews, headers, s3_key):
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(headers)
    for r in reviews:
        writer.writerow([r.get(k, "") for k in headers])
    s3 = boto3.client('s3')
    s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue())
    print(colored(f"✅ Uploaded CSV to s3://{BUCKET_NAME}/{s3_key}", "green"))

def get_slug_from_url(url):
    try:
        return url.strip().split('/')[4]
    except IndexError:
        return "place-" + datetime.today().strftime('%Y%m%d%H%M%S')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Google Maps reviews scraper.')
    parser.add_argument('--N', type=int, default=100, help='Number of reviews to scrape')
    parser.add_argument('--i', type=str, default='urls.txt', help='target URLs file')
    parser.add_argument('--o', type=str, default='output.csv', help='output CSV name for S3')
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
                    if "place_id:" in url:
                        place_id = url.split("place_id:")[-1]
                        url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
                    error = scraper.sort_by(url, ind[args.sort_by])

                    all_reviews = []

                    if error == 0:
                        offset = 0
                        while len(all_reviews) < args.N:
                            print(colored(f'[Fetching from offset {offset}]', 'cyan'))
                            reviews = scraper.get_reviews(offset, url)
                            if not reviews:
                                break

                            for r in reviews:
                                if len(all_reviews) >= args.N:
                                    break  # stop if we reached the desired number
                                r['retrieval_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                if args.source:
                                    r['source_url'] = url
                                all_reviews.append(r)

                            offset += len(reviews)


                        s3_key = f"{slug}/{args.o}"
                        headers = HEADER_W_SOURCE if args.source else HEADER
                        upload_csv_to_s3(all_reviews, headers, s3_key)
                    else:
                        print(colored(f'⚠️  Failed to sort reviews for {url}', 'red'))
