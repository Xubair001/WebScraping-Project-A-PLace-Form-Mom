import json
import math
from urllib.parse import urlencode

import pandas as pd
import scrapy
from scrapy.http import Response


class Mompider(scrapy.Spider):
    name = "mom"
    allowed_domains = ["aplaceformom.com"]

    # Custom settings (optional)
    custom_settings = {
        "USER_AGENT": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "LOG_ENABLED": True,  # Enable logging
        "LOG_LEVEL": "DEBUG",  # Set log level to INFO
        "LOG_FILE": "scrapy_output_mom.log",  # Log output to a file
        "HTTPERROR_ALLOW_ALL": True,
        "FEEDS": {
            "moms_urls.csv": {
                "format": "csv",
                "overwrite": "True",
                "encoding": "utf-8-sig",
            }
        },
    }

    def start_requests(self):
        self.base_url = "https://www.aplaceformom.com/"
        urls = [
            "https://www.aplaceformom.com/assisted-living",
            "https://www.aplaceformom.com/alzheimers-care",
            "https://www.aplaceformom.com/independent-living",
            "https://www.aplaceformom.com/senior-living",
            "https://www.aplaceformom.com/nursing-homes",
            "https://www.aplaceformom.com/home-care",
            "https://www.aplaceformom.com/senior-apartments",
            "https://www.aplaceformom.com/care-homes",
        ]

        cookies = {
            "_gcl_au": "1.1.1500820893.1733830856",
            "optimizelyEndUserId": "oeu1733830856376r0.008227620736852126",
            "_cs_c": "0",
            "lighthouse-homepage": "true",
            "requestURL": "https://www.aplaceformom.com/",
            "segmentSessionId": "1733830857105",
            "segmentLandingUrl": "https://www.aplaceformom.com/",
            "_ga": "GA1.1.1940571975.1733830857",
            "ajs_anonymous_id": "e2684855-e311-4b30-9f2a-ed55ef1d5d0b",
            "addshoppers.com": "2%7C1%3A0%7C10%3A1733830861%7C15%3Aaddshoppers.com%7C44%3AYjJlMTZlODVjYThmNGE4ZmFmZTBkYzJjYzExYmFiMDc%3D%7C8468035753b71d2e5e77f5746538c2c78d8dbc9deaaa09d581749bf4bb2383ff",
            "drift_campaign_refresh": "cb60e65f-36f4-417d-a701-a26633e7965d",
            "drift_aid": "2a98289d-c79f-4618-af2d-affb60fd0597",
            "driftt_aid": "2a98289d-c79f-4618-af2d-affb60fd0597",
            "server-response-time": "1479.001892ms",
            "shouldLoadAnalytics": "true",
            "_ga_ZN1SX8ZXV1": "GS1.1.1733830857.1.1.1733831072.0.0.0",
            "_cs_id": "564941bb-fc47-a533-cb03-a969be2a9196.1733830856.1.1733831072.1733830856.1.1767994856768.1",
            "_uetsid": "9f1bcdf0b6eb11ef923babaf4e1b795c",
            "_uetvid": "9f1be390b6eb11ef89bde35eff51306c",
            "_cs_s": "9.0.0.9.1733832873004",
            "optimizelySession": "1733831095932",
        }

        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en-GB;q=0.9,en;q=0.8",
            "cache-control": "max-age=0",
            # 'cookie': '_gcl_au=1.1.1500820893.1733830856; optimizelyEndUserId=oeu1733830856376r0.008227620736852126; _cs_c=0; lighthouse-homepage=true; requestURL=https://www.aplaceformom.com/; segmentSessionId=1733830857105; segmentLandingUrl=https://www.aplaceformom.com/; _ga=GA1.1.1940571975.1733830857; ajs_anonymous_id=e2684855-e311-4b30-9f2a-ed55ef1d5d0b; addshoppers.com=2%7C1%3A0%7C10%3A1733830861%7C15%3Aaddshoppers.com%7C44%3AYjJlMTZlODVjYThmNGE4ZmFmZTBkYzJjYzExYmFiMDc%3D%7C8468035753b71d2e5e77f5746538c2c78d8dbc9deaaa09d581749bf4bb2383ff; drift_campaign_refresh=cb60e65f-36f4-417d-a701-a26633e7965d; drift_aid=2a98289d-c79f-4618-af2d-affb60fd0597; driftt_aid=2a98289d-c79f-4618-af2d-affb60fd0597; server-response-time=1479.001892ms; shouldLoadAnalytics=true; _ga_ZN1SX8ZXV1=GS1.1.1733830857.1.1.1733831072.0.0.0; _cs_id=564941bb-fc47-a533-cb03-a969be2a9196.1733830856.1.1733831072.1733830856.1.1767994856768.1; _uetsid=9f1bcdf0b6eb11ef923babaf4e1b795c; _uetvid=9f1be390b6eb11ef89bde35eff51306c; _cs_s=9.0.0.9.1733832873004; optimizelySession=1733831095932',
            "if-none-match": '"4lmjgn89ch8wh"',
            "priority": "u=0, i",
            "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        }

        for url in urls:
            # Send the GET request
            yield scrapy.Request(
                url=url,
                headers=headers,
                callback=self.parse,
                cookies=cookies,
            )

    def parse(self, response: Response):
        # Extract required data from the response
        if response.status == 200:
            if response.xpath("//h2[contains(text(),'Top cities')]/../div[3]//a"):
                yield from response.follow_all(
                    urls=response.xpath(
                        "//h2[contains(text(),'Top cities')]/../div[3]//a/@href"
                    ).extract(),
                    callback=self.parse,
                )
            elif response.xpath("//div[@data-au-id='link-farm-module']//a"):
                yield from response.follow_all(
                    urls=response.xpath(
                        "//div[@data-au-id='link-farm-module']//a/@href"
                    ).extract(),
                    callback=self.parse,
                )
            else:
                print("Hello")
                property_section = response.xpath(
                    "//div[@class='CommunityCard__Header']"
                )
                for section in property_section:
                    property_url = section.xpath(
                        ".//a[contains(@class,'Address')]/@href"
                    ).extract_first()
                    property_name = section.xpath(
                        ".//h3/a[contains(@class,'Card')]/text()"
                    ).extract_first()
                    property_address = section.xpath(
                        ".//a[contains(@class,'Address')]/text()"
                    ).extract_first()
                    zip_code = (
                        ""
                        if not property_address
                        else property_address.split(",")[-1].split(" ")[-1]
                    )
                    yield {
                        "Property Name": property_name,
                        "Property Address": property_address,
                        "City": response.url.split("/")[-1].split("?")[0],
                        "Sate": response.url.split("/")[-2],
                        "Zip Code": zip_code,
                        "Category": response.url.split("/")[-3],
                        "URL": f"{self.base_url}{property_url}",
                    }

                if "destination-page" not in response.url:
                    pagination = int(
                        response.xpath(
                            "//div[@class='Pagination__Summary']//strong[2]/text()"
                        ).extract_first()
                    )
                    if pagination > 25:
                        pagination = math.ceil(pagination / 25)
                        pagination = [
                            f"{response.url}?destination-page={page}"
                            for page in range(2, pagination + 1)
                        ]
                        yield from response.follow_all(
                            urls=pagination,
                            callback=self.parse,
                        )
