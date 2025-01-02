import json
import os
import glob
import pandas as pd
from loguru import logger
import scrapy
from scrapy.http import JsonRequest, FormRequest  # FormRequest might help handle payload properly
from scrapy.crawler import CrawlerProcess
from scrapy import Spider
from scrapy.utils.project import get_project_settings


class ReviewsScraperSpider(scrapy.Spider):
    name = "reviews_scraper"
    custom_settings = {
        'FEED_FORMAT': "csv",
        "FEED_URI": "a_place_for_mom.csv"
    }

    def __init__(self, *args, **kwargs):
        super(ReviewsScraperSpider, self).__init__(*args, **kwargs)
        self.output_dir = "output"
        self.output_file = "A_Place_For_Mom.jl"
        self.reviews_endpoint = "https://www.aplaceformom.com/fragments/reviews-list"
        self.headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json;charset=UTF-8",
            "origin": "https://www.aplaceformom.com",
            "priority": "u=1, i",
            "referer": "https://www.aplaceformom.com",
            "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        }

        # Create the output directory and clean old files
        self._setup_output()

    def _setup_output(self):
        """Setup the output directory by cleaning old files, excluding .jl."""
        os.makedirs(self.output_dir, exist_ok=True)
        # Remove old .json files only
        old_files = glob.glob(os.path.join(self.output_dir, "*.json"))
        for file in old_files:
            try:
                os.remove(file)
                logger.info(f"Deleted old file: {file}")
            except Exception as e:
                logger.error(f"Failed to delete file {file}: {e}")

    def _extract_community_id(self, url):
        """Extract the community ID from the URL."""
        try:
            community_id = url.split("-")[-1]
            logger.info(f"Extracted community ID: {community_id}")
            return int(community_id)
        except ValueError as e:
            logger.error("Failed to extract community ID from URL.")
            raise Exception("Invalid URL structure for community ID.") from e

    def _extract_reviews(self, reviews_data):
        """Extract reviews from JSON data."""
        extracted_reviews = []
        for review in reviews_data.get("findManyReviews", {}).get("edges", []):
            review_data = {
                "reviewId": review.get("node", {}).get("reviewId"),
                "author_name": review.get("node", {}).get("reviewerDisplayName"),
                "rating": review.get("node", {}).get("overallRating"),
                "reviewTitle": review.get("node", {}).get("reviewTitle"),
                "review_text": review.get("node", {}).get("reviewContent"),
                "date": review.get("node", {}).get("createdAt"),
                "care_type": review.get("node", {}).get("relevantCareTypeName"),
            }
            yield review_data
        #     extracted_reviews.append(review_data)
        # return extracted_reviews

    def _save_data(self, data, page_number):
        """Save reviews data to a JSON file."""
        filename = os.path.join(self.output_dir, f"response_page_{page_number}.json")
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            logger.info(f"Saved data to {filename}")
        except IOError as e:
            logger.error(f"Failed to save data to {filename}: {e}")
            raise

    def _save_reviews_to_jl(self, reviews, url):
        """Save the extracted reviews to a JSON Lines (.jl) file."""
        try:
            with open(self.output_file, "a", encoding="utf-8") as jl_file:
                for review in reviews:
                    # Add the URL to each review
                    review["URL"] = url
                    json.dump(review, jl_file, ensure_ascii=False)
                    jl_file.write("\n")  # Write each review on a new line
            logger.info(f"Appended {len(reviews)} reviews to {self.output_file}")
        except IOError as e:
            logger.error(f"Error writing to file: {e}")
            raise

    def start_requests(self):
        """Start the request cycle from the URLs in the CSV."""
        df = pd.read_csv('moms_urls_updated.csv')
        urls = df['URL']
        for url in urls:
            community_id = self._extract_community_id(url)
            json_data = {
                "skip": 0,
                "yglCommunityId": community_id,
            }
            # Use FormRequest to handle JSON data (since JsonRequest might cause issues with some setups)
            yield FormRequest(
                url=self.reviews_endpoint,
                headers=self.headers,
                method="POST",
                body=json.dumps(json_data),  # Explicitly use json.dumps for body
                callback=self.parse_reviews,
                meta={'url': url, 'community_id': community_id, 'page': 1, 'reviews_count': 0}
            )

    def parse_reviews(self, response):
        """Parse the reviews data and handle pagination."""
        data = response.json()
        reviews_data = data.get("data", {})
        page_number = response.meta.get('page')
        reviews_count = response.meta.get('reviews_count')
        total_reviews = reviews_data.get("findManyReviews", {}).get("totalCount")
        # self._extract_reviews(reviews_data)
        for review in reviews_data.get("findManyReviews", {}).get("edges", []):
            review_data = {
                "reviewId": review.get("node", {}).get("reviewId"),
                "author_name": review.get("node", {}).get("reviewerDisplayName"),
                "rating": review.get("node", {}).get("overallRating"),
                "reviewTitle": review.get("node", {}).get("reviewTitle"),
                "review_text": review.get("node", {}).get("reviewContent"),
                "date": review.get("node", {}).get("createdAt"),
                "care_type": review.get("node", {}).get("relevantCareTypeName"),
                'url': response.meta['url']
            }
            yield review_data

        # Save data for this page
        # self._save_data(reviews_data, response.meta['page'])
        # self._save_reviews_to_jl(extracted_reviews, response.meta['url'])

        # Continue to the next page if needed
        if reviews_count < total_reviews:
            page_number += 1
            reviews_count += 20
            json_data = {
                "skip": reviews_count,
                "yglCommunityId": response.meta['community_id'],
                "currentPage": page_number,
            }
            yield FormRequest(
                url=self.reviews_endpoint,
                headers=self.headers,
                method="POST",
                body=json.dumps(json_data),  # Explicitly use json.dumps for body
                callback=self.parse_reviews,
                meta={'url': response.meta['url'], 'community_id': response.meta['community_id'], 'page': page_number, 'reviews_count': reviews_count}
            )

        logger.info(f"Successfully fetched and saved reviews for {response.meta['url']}")

if __name__ == "__main__":
    # Create the Scrapy process
    process = CrawlerProcess(get_project_settings())

    # Start the spider
    process.crawl(ReviewsScraperSpider)
    process.start()
