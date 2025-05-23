# -*- coding: utf-8 -*-
import itertools
import logging
import re
import time
import traceback
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import ChromeOptions as Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

GM_WEBPAGE = 'https://www.google.com/maps/'
MAX_WAIT = 10
MAX_RETRY = 5
MAX_SCROLLS = 40

class GoogleMapsScraper:

    def __init__(self, debug=False):
        self.debug = debug
        self.driver = self.__get_driver()
        self.logger = self.__get_logger()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        if exc_type is not None:
            traceback.print_exception(exc_type, exc_value, tb)

        self.driver.close()
        self.driver.quit()

        return True

    def sort_by(self, url, ind):

        self.driver.get(url)
        self.__click_on_cookie_agreement()

        wait = WebDriverWait(self.driver, MAX_WAIT)

        # open dropdown menu
        clicked = False
        tries = 0
        while not clicked and tries < MAX_RETRY:
            try:
                menu_bt = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[@data-value=\'Sort\']')))
                menu_bt.click()

                clicked = True
                time.sleep(3)
            except Exception as e:
                tries += 1
                self.logger.warn('Failed to click sorting button')

            # failed to open the dropdown
            if tries == MAX_RETRY:
                return -1

        #  element of the list specified according to ind
        recent_rating_bt = self.driver.find_elements(By.XPATH, '//div[@role=\'menuitemradio\']')[ind]
        recent_rating_bt.click()

        # wait to load review (ajax call)
        time.sleep(4)

        return 0

    def get_places(self, keyword_list=None):

        df_places = pd.DataFrame()
        search_point_url_list = self._gen_search_points_from_square(keyword_list=keyword_list)

        for i, search_point_url in enumerate(search_point_url_list):
            print(search_point_url)

            if (i+1) % 10 == 0:
                print(f"{i}/{len(search_point_url_list)}")
                df_places = df_places[['search_point_url', 'href', 'name', 'rating', 'num_reviews', 'close_time', 'other']]
                df_places.to_csv('output/places_wax.csv', index=False)


            try:
                self.driver.get(search_point_url)
            except NoSuchElementException:
                self.driver.quit()
                self.driver = self.__get_driver()
                self.driver.get(search_point_url)

            # scroll to load all (20) places into the page
            scrollable_div = self.driver.find_element(By.CSS_SELECTOR,
                "div.m6QErb.DxyBCb.kA9KIf.dS8AEf.ecceSd > div[aria-label*='Results for']")
            for i in range(10):
                self.driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)

            # Get places names and href
            time.sleep(2)
            response = BeautifulSoup(self.driver.page_source, 'html.parser')
            div_places = response.select('div[jsaction] > a[href]')

            for div_place in div_places:
                place_info = {
                    'search_point_url': search_point_url.replace('https://www.google.com/maps/search/', ''),
                    'href': div_place['href'],
                    'name': div_place['aria-label']
                }

                df_places = df_places.append(place_info, ignore_index=True)

            # TODO: implement click to handle > 20 places

        df_places = df_places[['search_point_url', 'href', 'name']]
        df_places.to_csv('output/places_wax.csv', index=False)



    # def get_reviews(self, offset):
    #
    #     # scroll to load reviews
    #     self.__scroll()
    #
    #     # wait for other reviews to load (ajax)
    #     time.sleep(4)
    #
    #     # expand review text
    #     self.__expand_reviews()
    #
    #     # parse reviews
    #     response = BeautifulSoup(self.driver.page_source, 'html.parser')
    #     # TODO: Subject to changes
    #     rblock = response.find_all('div', class_='jftiEf fontBodyMedium')
    #     parsed_reviews = []
    #     for index, review in enumerate(rblock):
    #         if index >= offset:
    #             r = self.__parse(review)
    #             parsed_reviews.append(r)
    #
    #             # logging to std out
    #             print(r)
    #
    #     return parsed_reviews

    # def get_reviews(self, offset, url):
    #     """
    #     Scrape reviews and include the Place ID in the review metadata.

    #     Parameters:
    #         offset (int): The starting point for reviews to scrape.
    #         url (str): The URL containing the Place ID.

    #     Returns:
    #         list[dict]: List of reviews with metadata, including Place ID.
    #     """
    #     # Scroll to load reviews
    #     self.__scroll()

    #     # Wait for other reviews to load (ajax)
    #     time.sleep(5)

    #     # Expand review text
    #     self.__expand_reviews()

    #     # Extract Place ID from URL
    #     place_id = self.extract_place_id_from_url(url)

    #     # Parse reviews
    #     response = BeautifulSoup(self.driver.page_source, 'html.parser')
    #     rblock = response.find_all('div', class_='jftiEf fontBodyMedium')
    #     parsed_reviews = []

    #     for index, review in enumerate(rblock):
    #         if index >= offset:
    #             r = self.__parse(review)
    #             r['place_id'] = place_id  # Add Place ID to each review's metadata
    #             parsed_reviews.append(r)

    #             # Logging to stdout
    #             print(r)

    #     return parsed_reviews

    def get_reviews(self, offset, url):
        """
        Scrape reviews and include the Place ID in the review metadata.

        Parameters:
            offset (int): The starting point for reviews to scrape.
            url (str): The URL containing the Place ID.

        Returns:
            list[dict]: List of reviews with metadata, including Place ID.
        """
        self.__scroll()  # Intelligent scroll with retry logic
        time.sleep(5)
        self.__expand_reviews()

        place_id = self.extract_place_id_from_url(url)

        response = BeautifulSoup(self.driver.page_source, 'html.parser')
        rblock = response.find_all('div', class_='jftiEf fontBodyMedium')
        parsed_reviews = []

        for index, review in enumerate(rblock):
            if index >= offset:
                r = self.__parse(review)
                if not r.get('id_review'):
                    self.logger.warning("Skipped a review block due to missing ID.")
                r['place_id'] = place_id
                parsed_reviews.append(r)
                print(r)

        return parsed_reviews



    def extract_place_id_from_url(self, url):
        """
        Extract the Place ID from a given Google Maps URL.

        Parameters:
            url (str): The Google Maps URL containing the Place ID.

        Returns:
            str: The extracted Place ID or None if not found.
        """
        import re
        try:
            if "q=place_id:" in url:
                place_id = re.search(r'q=place_id:([^&]+)', url).group(1)
                return place_id
            return None
        except Exception as e:
            self.logger.warning(f"Failed to extract Place ID from URL {url}: {e}")
            return None

    # need to use different url wrt reviews one to have all info
    def get_account(self, url):

        self.driver.get(url)
        self.__click_on_cookie_agreement()

        # ajax call also for this section
        time.sleep(2)

        resp = BeautifulSoup(self.driver.page_source, 'html.parser')

        place_data = self.__parse_place(resp, url)
        # Add Place ID from URL
        place_id = self.extract_place_id_from_url(url)
        place_data['place_id'] = place_id

        return place_data

    def __parse(self, review):

        item = {}

        try:
            # TODO: Subject to changes
            id_review = review['data-review-id']
        except Exception as e:
            id_review = None

        if id_review is None:
            self.logger.warning("Skipped a review block due to missing ID.")


        try:
            # TODO: Subject to changes
            username = review['aria-label']
        except Exception as e:
            username = None

        try:
            # TODO: Subject to changes
            place_id = review['place_id']
        except Exception as e:
            place_id = None

        try:
            # TODO: Subject to changes
            review_text = self.__filter_string(review.find('span', class_='wiI7pd').text)
        except Exception as e:
            review_text = None

        try:
            # TODO: Subject to changes
            rating = float(review.find('span', class_='kvMYJc')['aria-label'].split(' ')[0])
        except Exception as e:
            rating = None

        # try:
        #     # TODO: Subject to changes
        #     relative_date = review.find('span', class_='rsqaWe').text
        # except Exception as e:
        #     relative_date = None

        try:
            # Extract relative date
            relative_date = review.find('span', class_='rsqaWe').text

            # Convert relative date to an actual date
            retrieval_date = datetime.now()
            if "day" in relative_date:
                days_ago = int(relative_date.split()[0])
                review_date = retrieval_date - timedelta(days=days_ago)
            elif "week" in relative_date:
                weeks_ago = int(relative_date.split()[0])
                review_date = retrieval_date - timedelta(weeks=weeks_ago)
            elif "month" in relative_date:
                months_ago = int(relative_date.split()[0])
                review_date = retrieval_date - relativedelta(months=months_ago)
            elif "year" in relative_date:
                years_ago = int(relative_date.split()[0])
                review_date = retrieval_date - relativedelta(years=years_ago)
            else:
                review_date = retrieval_date  # Default to retrieval date if unknown format
        except Exception as e:
            relative_date = None
            review_date = None

        try:
            n_reviews = review.find('div', class_='RfnDt').text.split(' ')[3]
        except Exception as e:
            n_reviews = 0

        try:
            user_url = review.find('button', class_='WEBjve')['data-href']
        except Exception as e:
            user_url = None

        item['id_review'] = id_review
        item['caption'] = review_text

        # depends on language, which depends on geolocation defined by Google Maps
        # custom mapping to transform into date should be implemented
        item['relative_date'] = relative_date
        item['review_date'] = review_date
        # store datetime of scraping and apply further processing to calculate
        # correct date as retrieval_date - time(relative_date)
        item['retrieval_date'] = datetime.now()
        item['rating'] = rating
        item['username'] = username
        item['n_review_user'] = n_reviews
        #item['n_photo_user'] = n_photos  ## not available anymore
        #item['url_user'] = user_url
        item['place_id'] = place_id

        return item


    def __parse_place(self, response, url):

        place = {}

        try:
            place['name'] = response.find('h1', class_='DUwDvf fontHeadlineLarge').text.strip()
        except Exception as e:
            place['name'] = None

        try:
            place['overall_rating'] = float(response.find('div', class_='F7nice ').find('span', class_='ceNzKf')['aria-label'].split(' ')[1])
        except Exception as e:
            place['overall_rating'] = None

        try:
            place['n_reviews'] = int(response.find('div', class_='F7nice ').text.split('(')[1].replace(',', '').replace(')', ''))
        except Exception as e:
            place['n_reviews'] = 0

        try:
            place['n_photos'] = int(response.find('div', class_='YkuOqf').text.replace('.', '').replace(',','').split(' ')[0])
        except Exception as e:
            place['n_photos'] = 0

        try:
            place['category'] = response.find('button', jsaction='pane.rating.category').text.strip()
        except Exception as e:
            place['category'] = None

        try:
            place['description'] = response.find('div', class_='PYvSYb').text.strip()
        except Exception as e:
            place['description'] = None

        b_list = response.find_all('div', class_='Io6YTe fontBodyMedium')
        try:
            place['address'] = b_list[0].text
        except Exception as e:
            place['address'] = None

        try:
            place['website'] = b_list[1].text
        except Exception as e:
            place['website'] = None

        try:
            place['phone_number'] = b_list[2].text
        except Exception as e:
            place['phone_number'] = None
    
        try:
            place['plus_code'] = b_list[3].text
        except Exception as e:
            place['plus_code'] = None

        try:
            place['opening_hours'] = response.find('div', class_='t39EBf GUrTXd')['aria-label'].replace('\u202f', ' ')
        except:
            place['opening_hours'] = None

        place['url'] = url

        lat, long, z = url.split('/')[6].split(',')
        place['lat'] = lat[1:]
        place['long'] = long

        return place


    def _gen_search_points_from_square(self, keyword_list=None):
        # TODO: Generate search points from corners of square

        keyword_list = [] if keyword_list is None else keyword_list

        square_points = pd.read_csv('input/square_points.csv')

        cities = square_points['city'].unique()

        search_urls = []

        for city in cities:

            df_aux = square_points[square_points['city'] == city]
            latitudes = df_aux['latitude'].unique()
            longitudes = df_aux['longitude'].unique()
            coordinates_list = list(itertools.product(latitudes, longitudes, keyword_list))

            search_urls += [f"https://www.google.com/maps/search/{coordinates[2]}/@{str(coordinates[1])},{str(coordinates[0])},{str(15)}z"
             for coordinates in coordinates_list]

        return search_urls


    # expand review description
    def __expand_reviews(self):
        # use XPath to load complete reviews
        # TODO: Subject to changes
        buttons = self.driver.find_elements(By.CSS_SELECTOR,'button.w8nwRe.kyuRq')
        for button in buttons:
            self.driver.execute_script("arguments[0].click();", button)


    # def __scroll(self):
    #     # TODO: Subject to changes
    #     scrollable_div = self.driver.find_element(By.CSS_SELECTOR,'div.m6QErb.DxyBCb.kA9KIf.dS8AEf')
    #     self.driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
    #     #self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    # def __scroll(self):
    #     try:
    #         # ✅ Wait for reviews container before scrolling
    #         scrollable_div = WebDriverWait(self.driver, 10).until(
    #             EC.presence_of_element_located((By.CSS_SELECTOR, 'div.m6QErb.DxyBCb.kA9KIf.dS8AEf'))
    #         )
    #         self.driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
    #         time.sleep(2)  # Allow time for reviews to load
    #     except Exception as e:
    #         self.logger.error(f"Scrolling failed: {e}")

    def __scroll(self, max_scrolls=40):
        try:
            scrollable_div = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.m6QErb.DxyBCb.kA9KIf.dS8AEf'))
            )

            last_height = self.driver.execute_script("return arguments[0].scrollHeight", scrollable_div)
            scroll_attempts = 0
            no_change_count = 0

            while scroll_attempts < max_scrolls:
                self.driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
                time.sleep(2)  # Allow time for AJAX to load new reviews

                new_height = self.driver.execute_script("return arguments[0].scrollHeight", scrollable_div)
                if new_height == last_height:
                    no_change_count += 1
                    if no_change_count >= 3:
                        break  # Stop if no new reviews loaded after 3 attempts
                else:
                    no_change_count = 0
                    last_height = new_height

                scroll_attempts += 1

            print(f"✅ Completed scrolling in {scroll_attempts} attempts.")

        except Exception as e:
            self.logger.error(f"Scrolling failed: {e}")



    def __get_logger(self):
        # create logger
        logger = logging.getLogger('googlemaps-scraper')
        logger.setLevel(logging.DEBUG)

        # create console handler and set level to debug
        fh = logging.FileHandler('gm-scraper.log')
        fh.setLevel(logging.DEBUG)

        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # add formatter to ch
        fh.setFormatter(formatter)

        # add ch to logger
        logger.addHandler(fh)

        return logger


    def __get_driver(self, debug=False):
        options = Options()

        if not self.debug:
            options.add_argument("--headless")
        else:
            options.add_argument("--window-size=1366,768")

        options.add_argument("--disable-notifications")
        #options.add_argument("--lang=en-GB")
        options.add_argument("--accept-lang=en-GB")
        input_driver = webdriver.Chrome(service=Service(), options=options)

         # click on google agree button so we can continue (not needed anymore)
         # EC.element_to_be_clickable((By.XPATH, '//span[contains(text(), "I agree")]')))
        input_driver.get(GM_WEBPAGE)

        return input_driver

    # cookies agreement click
    def __click_on_cookie_agreement(self):
        try:
            agree = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//span[contains(text(), "Reject all")]')))
            agree.click()

            # back to the main page
            # self.driver.switch_to_default_content()

            return True
        except:
            return False

    # util function to clean special characters
    def __filter_string(self, str):
        strOut = str.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
        return strOut
