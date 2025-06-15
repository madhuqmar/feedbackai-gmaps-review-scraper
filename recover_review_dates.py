# recover_review_dates.py
import pandas as pd
import boto3
import io
from datetime import datetime
from googlemaps import GoogleMapsScraper

# === CONFIG ===
BUCKET_NAME = 'naturals-reviews'
SOURCE_KEY = 'combined/all_4_naturals_salons.csv'
MISSING_FILE_LOCAL = 'reviews_missing_dates.csv'
RECOVERED_FILE_LOCAL = 'recovered_review_dates.csv'
S3_OUTPUT_KEY = f'monitoring/{RECOVERED_FILE_LOCAL}'

# === STEP 1: Load full review CSV from S3 ===
s3 = boto3.client('s3')

try:
    response = s3.get_object(Bucket=BUCKET_NAME, Key=SOURCE_KEY)
    df = pd.read_csv(io.StringIO(response['Body'].read().decode('utf-8')))
    print(f"✅ Loaded {len(df)} total reviews from S3.")
except Exception as e:
    print(f"❌ Failed to read from S3: {e}")
    exit()

# === STEP 2: Filter reviews missing review_date ===
missing_df = df[df['review_date'].isna()]
print(f"🔍 Found {len(missing_df)} reviews missing `review_date`.")

missing_df.to_csv(MISSING_FILE_LOCAL, index=False)
print(f"📄 Saved missing reviews to: {MISSING_FILE_LOCAL}")

try:
    with open(MISSING_FILE_LOCAL, 'rb') as f:
        s3.upload_fileobj(f, BUCKET_NAME, f'monitoring/{MISSING_FILE_LOCAL}')
    print(f"☁️ Uploaded missing list to s3://{BUCKET_NAME}/monitoring/{MISSING_FILE_LOCAL}")
except Exception as e:
    print(f"⚠️ Upload of missing list failed: {e}")

# === STEP 3: Scrape to recover missing review dates ===
recovered_reviews = []

with GoogleMapsScraper(debug=False) as scraper:
    for _, row in missing_df.iterrows():
        review_id = row['id_review']
        place_id = row['place_id']
        url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"

        err = scraper.sort_by(url, 1)
        if err != 0:
            print(f"⚠️ Failed to sort reviews for {place_id}")
            continue

        offset = 0
        found = False

        while not found:
            reviews = scraper.get_reviews(offset, url)
            if not reviews:
                break

            for r in reviews:
                # DEBUG: Print to see what's in each review object
                print(f"🔎 Review ID: {r.get('id_review')}, Available Keys: {r.keys()}")

                if r.get('id_review') == review_id:
                    # Attempt to extract date information
                    relative_date = r.get('relative_date') or r.get('relative_time_description') or ''
                    review_date = r.get('review_date') or r.get('time') or ''

                    # Optional: Convert UNIX timestamp to date
                    if isinstance(review_date, (int, float)):
                        try:
                            review_date = datetime.utcfromtimestamp(review_date).strftime('%Y-%m-%d')
                        except:
                            review_date = ''

                    recovered_reviews.append({
                        'id_review': review_id,
                        'review_date': review_date,
                        'relative_date': relative_date,
                        'retrieval_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                    found = True
                    break

            offset += len(reviews)

        if not found:
            print(f"❌ Review {review_id} not found for place_id {place_id}")

# === STEP 4: Save recovered data ===
recovered_df = pd.DataFrame(recovered_reviews)
recovered_df.to_csv(RECOVERED_FILE_LOCAL, index=False)
print(f"✅ Recovered {len(recovered_df)} reviews. Saved to: {RECOVERED_FILE_LOCAL}")

try:
    with open(RECOVERED_FILE_LOCAL, 'rb') as f:
        s3.upload_fileobj(f, BUCKET_NAME, S3_OUTPUT_KEY)
    print(f"☁️ Uploaded recovered reviews to s3://{BUCKET_NAME}/{S3_OUTPUT_KEY}")
except Exception as e:
    print(f"⚠️ Upload failed: {e}")
