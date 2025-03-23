import scrapy
import json
import csv
import os
from datetime import datetime
from scrapy.selector import Selector
from scrapy import Spider, Request
from scrapy_selenium import SeleniumRequest
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
import time
import re

class GetdataSpider(scrapy.Spider):
    name = "getdata"
    allowed_domains = ["www.airbnb.ch"]
    max_apartments = 100
    results_by_url = {}
    processed_urls = set()
    current_url = None
    max_retries_per_page = 3

    def __init__(self):
        super().__init__()
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.results_dir = 'airbnb_results'
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)

    def start_requests(self):
        base_urls = [
            'https://www.airbnb.ch/s/St.-Gallen--Schweiz/homes?refinement_paths%5B%5D=%2Fhomes&checkin=2025-06-26&checkout=2025-06-29&date_picker_type=calendar&search_type=filter_change&query=St.%20Gallen%2C%20Schweiz&place_id=ChIJVdgzdikem0cRFGH-HwhQIpo&flexible_trip_lengths%5B%5D=one_week&monthly_start_date=2025-04-01&monthly_length=3&monthly_end_date=2025-07-01&search_mode=regular_search&price_filter_input_type=2&price_filter_num_nights=3&channel=EXPLORE&source=structured_search_input_header&pagination_search=true',
            'https://www.airbnb.ch/s/St.-Gallen--Schweiz/homes?refinement_paths%5B%5D=%2Fhomes&checkin=2025-10-09&checkout=2025-10-19&date_picker_type=calendar&search_type=AUTOSUGGEST'
        ]
        
        for url in base_urls:
            if url not in self.processed_urls:
                self.current_url = url
                self.results_by_url[url] = []
                self.processed_urls.add(url)
                self.logger.info(f"Processing URL: {url}")
                self.driver.get(url)
                time.sleep(5)  # Initial wait for dynamic content
                yield from self.parse_page(url)

    def scroll_and_wait(self, scroll_pause_time=2):
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        attempts = 0
        max_attempts = 10  # Increase maximum scroll attempts
        
        while attempts < max_attempts:
            # Scroll in smaller increments (8 steps instead of 4)
            for i in range(8):
                self.driver.execute_script(f"window.scrollBy(0, {last_height/8});")
                time.sleep(0.8)  # Increased wait time during scroll
            
            # Wait longer for content to load
            time.sleep(scroll_pause_time)
            
            # Try to trigger lazy loading by moving mouse (simulate user behavior)
            self.driver.execute_script("window.scrollBy(0, -100);")
            time.sleep(0.5)
            self.driver.execute_script("window.scrollBy(0, 100);")
            
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            
            if new_height == last_height:
                # Wait longer and try one more scroll
                time.sleep(scroll_pause_time * 2)
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    attempts += 1
                    if attempts >= max_attempts:
                        break
            
            last_height = new_height

    def get_listings(self, max_attempts=5):
        listings = []
        attempt = 0
        
        while attempt < max_attempts and len(listings) == 0:
            try:
                listings = self.driver.find_elements(By.CSS_SELECTOR, 'div[itemprop="itemListElement"]')
                if len(listings) == 0:
                    time.sleep(2)
                    attempt += 1
            except Exception as e:
                self.logger.error(f"Error getting listings on attempt {attempt + 1}: {str(e)}")
                time.sleep(2)
                attempt += 1
        
        return listings

    def find_next_button(self):
        max_attempts = 3
        attempt = 0
        while attempt < max_attempts:
            selectors = [
                'a[aria-label="Next"]',
                'a[aria-label="Nächste"]',
                'button[aria-label="Next"]',
                'button[aria-label="Nächste"]',
                '[data-testid="pagination-next-btn"]',
                'button._1mzhry13',  # Common Airbnb next button class
                '*[aria-label*="next"]',
                '*[aria-label*="nächste"]',
                'a._1bfat5l',  # Another common Airbnb pagination class
                'button[aria-label*="pagination-button-next"]'
            ]
            
            for selector in selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        if element.is_enabled() and element.is_displayed():
                            try:
                                # Additional verification that the button is clickable
                                if not any(disabled_class in element.get_attribute("class").lower() 
                                         for disabled_class in ["disabled", "inactive", "_disabled"]):
                                    # Scroll the button into view
                                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                                    time.sleep(1)
                                    return element
                            except:
                                continue
                except Exception as e:
                    self.logger.debug(f"Error finding next button with selector {selector}: {str(e)}")
                    continue
            
            attempt += 1
            if attempt < max_attempts:
                time.sleep(2)
                self.scroll_and_wait()
        
        return None

    def extract_pagination_info(self, url):
        """Extract and update pagination parameters from URL"""
        try:
            items_offset = 0
            if 'items_offset=' in url:
                offset_match = re.search(r'items_offset=(\d+)', url)
                if offset_match:
                    items_offset = int(offset_match.group(1))
            
            # Remove existing offset if present
            base_url = re.sub(r'&items_offset=\d+', '', url)
            if '?' not in base_url:
                base_url += '?'
            elif not base_url.endswith('&') and not base_url.endswith('?'):
                base_url += '&'
            
            return base_url, items_offset
        except Exception as e:
            self.logger.error(f"Error extracting pagination info: {str(e)}")
            return url, 0

    def get_next_page_url(self, current_url, items_found):
        """Generate next page URL using offset-based pagination"""
        try:
            base_url, current_offset = self.extract_pagination_info(current_url)
            
            # Airbnb typically shows 20 items per page
            next_offset = current_offset + items_found
            
            # If we got fewer items than expected, increase offset by standard page size
            if items_found < 15:
                next_offset = current_offset + 20
            
            next_url = f"{base_url}items_offset={next_offset}"
            
            # Add additional parameters if not present
            if 'pagination_search=true' not in next_url:
                next_url += '&pagination_search=true'
            if 'section_offset' not in next_url:
                next_url += f'&section_offset={next_offset}'
            
            return next_url
        except Exception as e:
            self.logger.error(f"Error generating next page URL: {str(e)}")
            return None

    def parse_page(self, initial_url, retry_count=0):
        try:
            if len(self.results_by_url[initial_url]) >= self.max_apartments:
                self.logger.info(f"Reached {self.max_apartments} listings for {initial_url}")
                self.save_results(initial_url)
                return

            time.sleep(5)
            # Scroll multiple times to ensure all content is loaded
            for _ in range(3):
                self.scroll_and_wait()
                time.sleep(2)

            listings = self.get_listings()
            current_page_results = []
            initial_count = len(listings)
            
            # If we got very few listings, try refreshing and scrolling again
            if initial_count < 15 and retry_count < self.max_retries_per_page:
                self.logger.info(f"Got only {initial_count} listings, retrying page load...")
                self.driver.refresh()
                time.sleep(5)
                self.scroll_and_wait()
                listings = self.get_listings()
            
            for listing in listings:
                try:
                    if len(self.results_by_url[initial_url]) >= self.max_apartments:
                        self.logger.info(f"Reached target of {self.max_apartments} listings")
                        self.save_results(initial_url)
                        return

                    # Try to get the full listing title
                    name = None
                    # Look for the meta title first
                    try:
                        name = listing.find_element(By.CSS_SELECTOR, 'meta[itemprop="name"]').get_attribute('content')
                    except NoSuchElementException:
                        pass
                    
                    if not name:
                        # Look for the div containing the full title
                        title_selectors = [
                            'div[data-testid="listing-card-title"] div',
                            'div[data-testid="listing-card-title"] span',
                            'div[data-section-id="title"]',
                            'span[data-testid="listing-card-name"]',
                            'div[style*="--title-name"]',
                            'div[itemprop="name"]',
                            'div[data-testid*="title"]',
                            'div[role="heading"]'
                        ]
                        
                        for selector in title_selectors:
                            try:
                                elements = listing.find_elements(By.CSS_SELECTOR, selector)
                                if elements:
                                    texts = [e.text.strip() for e in elements if e.text.strip()]
                                    if texts:
                                        name = max(texts, key=len)
                                        break
                            except:
                                continue
                    
                    if not name:
                        continue
                    
                    price = None
                    try:
                        price_selectors = [
                            'div[data-testid*="price-line"]',
                            'div[data-testid*="price"]',
                            'span[data-testid*="price"]',
                            'div._1jo4hgw',  # Common Airbnb price class
                            'span._tyxjp1'   # Alternative price class
                        ]
                        
                        for selector in price_selectors:
                            try:
                                price_element = listing.find_element(By.CSS_SELECTOR, selector)
                                price_text = price_element.text
                                price_match = re.search(r'(\d+)\s*CHF\s+pro\s+Nacht', price_text)
                                if price_match:
                                    price = f"{price_match.group(1)} CHF"
                                    break
                            except:
                                continue
                    except Exception as e:
                        self.logger.error(f"Error extracting price: {str(e)}")
                        continue

                    if name and price:
                        data = {
                            'name': name.strip(),
                            'price_per_night': price.strip()
                        }
                        if data not in self.results_by_url[initial_url]:  # Avoid duplicates
                            self.results_by_url[initial_url].append(data)
                            current_page_results.append(data)
                            self.logger.info(f"Found listing {len(self.results_by_url[initial_url])}/{self.max_apartments}: {name} - Price per night: {price}")

                except Exception as e:
                    self.logger.error(f"Error processing listing: {str(e)}")
                    continue

            # Check if we need to continue pagination
            remaining_needed = self.max_apartments - len(self.results_by_url[initial_url])
            if remaining_needed > 0:
                self.logger.info(f"Need {remaining_needed} more listings to reach target of {self.max_apartments}")
                next_button = self.find_next_button()
                if next_button:
                    try:
                        self.logger.info("Found next button, attempting to click...")
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                        time.sleep(2)
                        next_button.click()
                        time.sleep(5)  # Wait for page load
                        
                        # Verify page changed
                        current_url = self.driver.current_url
                        if current_url not in self.processed_urls:
                            self.processed_urls.add(current_url)
                            yield from self.parse_page(initial_url, 0)  # Reset retry count for new page
                        else:
                            self.logger.warning("Page URL didn't change after clicking next, might be stuck")
                            if retry_count < self.max_retries_per_page:
                                self.logger.info("Retrying current page...")
                                yield from self.parse_page(initial_url, retry_count + 1)
                            else:
                                self.logger.warning("Max retries reached, saving current results")
                                self.save_results(initial_url)
                    except Exception as e:
                        self.logger.error(f"Error navigating to next page: {str(e)}")
                        if retry_count < self.max_retries_per_page:
                            yield from self.parse_page(initial_url, retry_count + 1)
                        else:
                            self.save_results(initial_url)
                else:
                    self.logger.info(f"No next button found, ending with {len(self.results_by_url[initial_url])} listings")
                    self.save_results(initial_url)
            else:
                self.logger.info(f"Successfully collected {len(self.results_by_url[initial_url])} listings")
                self.save_results(initial_url)

        except Exception as e:
            self.logger.error(f"Error during page parsing: {str(e)}")
            if retry_count < self.max_retries_per_page:
                yield from self.parse_page(initial_url, retry_count + 1)
            else:
                self.save_results(initial_url)

    def save_results(self, url):
        if not self.results_by_url[url]:
            return
            
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        search_number = len(os.listdir(self.results_dir)) + 1
        
        # Save as CSV
        csv_filename = os.path.join(self.results_dir, f'airbnb_results_search_{search_number}_{timestamp}.csv')
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['name', 'price_per_night']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for item in self.results_by_url[url]:
                writer.writerow(item)
        
        # Also save as JSON for backup
        json_filename = os.path.join(self.results_dir, f'airbnb_results_search_{search_number}_{timestamp}.json')
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(self.results_by_url[url], f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"Saved {len(self.results_by_url[url])} results to {csv_filename} and {json_filename}")
        
    def closed(self, reason):
        if hasattr(self, 'driver'):
            self.driver.quit()
        # Save any remaining results
        for url in self.results_by_url:
            if self.results_by_url[url]:
                self.save_results(url)
