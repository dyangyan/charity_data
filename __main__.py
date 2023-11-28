### Setup environment
import certifi
import csv
import math
import random
import re
import requests
import time

from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


### Global variables
MONGODB_USERNAME = "danyangyan"
MONGODB_PASSWORD = "5JhJMQ2itEnVW2Wm"
MONGODB_CONNECTION = (
    "mongodb+srv://"
    + mongodb_username
    + ":"
    + mongodb_password
    + "@discourse.mwgzz8m.mongodb.net"
)
CRA_PARENT_URL = "https://apps.cra-arc.gc.ca"


### Helper methods
def random_sleep(lower, upper):
    """Sleep with random delay."""
    delay = random.uniform(lower, upper)
    time.sleep(delay)


def load_html(url):
    """Extract html payload from url."""
    try:
        html_content = requests.get(url, timeout=10)
        html_content.raise_for_status()
        output = BeautifulSoup(html_content.text, "html.parser")

        return output

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")

        return -1


def connect_to_mongodb(connection, collection_name):
    """Return MongoDB collection."""
    # Connect to MongoDB
    client = MongoClient(connection, tlsCAFile=certifi.where())

    # Select the database (create one if it doesn't exist)
    db = client["charity_data"]

    # Return selected or created collection
    return client, db[collection_name]


def export_to_csv(file_name, input_data):
    """Export dictionaries to CSV."""
    todays_date = datetime.today().strftime("%Y-%m-%d")
    file_path = (
        "/Users/danyan/Library/CloudStorage/OneDrive-Personal/1-Education/code-workspace/charity_data/exports/"
        + todays_date
        + "-"
        + file_name
        + ".csv"
    )

    with open(file_path, "w", newline="", encoding="utf-8") as csv_file:
        fieldnames = input_data[0].keys()  # Assumes all dictionaries have the same keys
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # Write the header
        writer.writeheader()

        # Write the data
        writer.writerows(input_data)


def export_to_mongodb(connection, collection_name, input_data):
    """Export data to MongoDB."""
    client, collection = connect_to_mongodb(connection, collection_name)

    # Insert the list of documents into the collection
    collection.insert_many(input_data)

    # Close the connection
    client.close()


def get_max_page_increment(soup):
    """
    Return the max page increment for data displayed over multiple pages.
    Usually pages with the form 'Showing 101 to 200 of 676 entries on this page'
    """
    pages_indicator_pattern = re.compile(
        r"Showing \d+ to \d+ of \d+ entries on this page"
    )
    pages_indicator = soup.find(
        lambda tag: tag.name == "strong"
        and pages_indicator_pattern.search(tag.get_text(strip=True))
    )

    pattern = re.compile(r"\d+")  # Define a regular expression pattern to match numbers

    matches = pattern.findall(
        pages_indicator
    )  # Find all matches in the pages_indicator text

    if len(matches) >= 3:
        items_in_page = int(matches[1])
        page_count = math.ceil(
            int(matches[2]) / items_in_page
        )  # Pages = total entries / entries per page, rounded up

    return items_in_page, page_count


### Data extraction methods
def load_charity_list():
    """Load list of all charities."""
    # Set up source and lists to receive load
    target_url = "https://apps.cra-arc.gc.ca/ebci/hacc/srch/pub/bscSrch"
    page_num = 1

    master_charity_name = []
    master_charity_url = []
    master_charity_status = []
    master_charity_type = []
    master_charity_province = []
    master_charity_city = []
    master_charity_status_date = []

    # Load html data
    soup = load_html(target_url)
    items_in_page, page_count = get_max_page_increment(soup)

    # While a next page exists, append table data to master lists
    while soup != -1 and page_num <= page_count:
        ### Gather output table data including charity name, url, status, etc.
        # Find all td elements with headers="headername", "header2" through "header6"
        headername_tds = soup.find_all("td", {"headers": "headername"})
        header2_tds = soup.find_all("td", {"headers": "header2"})
        header3_tds = soup.find_all("td", {"headers": "header3"})
        header4_tds = soup.find_all("td", {"headers": "header4"})
        header5_tds = soup.find_all("td", {"headers": "header5"})
        header6_tds = soup.find_all("td", {"headers": "header6"})

        # Extract text and href from each <a> element and store in lists
        charity_name = [
            td.find("a").text.strip() for td in headername_tds if td.find("a")
        ]
        charity_url = [
            td.find("a")["href"].strip() for td in headername_tds if td.find("a")
        ]

        # Extract text from each td element and store in lists
        charity_status = [td.text.strip() for td in header2_tds]
        charity_type = [td.text.strip() for td in header3_tds]
        charity_province = [td.text.strip() for td in header4_tds]
        charity_city = [td.text.strip() for td in header5_tds]
        charity_status_date = [td.text.strip() for td in header6_tds]

        # Append lists to master lists
        master_charity_name.extend(charity_name)
        master_charity_url.extend(charity_url)
        master_charity_status.extend(charity_status)
        master_charity_type.extend(charity_type)
        master_charity_province.extend(charity_province)
        master_charity_city.extend(charity_city)
        master_charity_status_date.extend(charity_status_date)

        # Increment pagination
        page_num = page_num + 1

        target_url = (
            "https://apps.cra-arc.gc.ca/ebci/hacc/srch/pub/bscSrch?dsrdPg="
            + str(page_num)
            + "&q.stts=0007&q.ordrClmn=NAME&q.ordrRnk=ASC"
        )

        # Load next page - if no response, exit loop (assuming reached last avail. page)
        soup = load_html(target_url)

        # Pause bc website is weak af
        random_sleep(1, 12)

    # Combine lists into a list of dictionaries, adding insert date
    insert_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    documents = [
        {
            "charity_name": name_text,
            "charity_url": name_href,
            "charity_status": status,
            "charity_type": charity_type,
            "charity_province": province,
            "charity_city": city,
            "charity_status_date": status_date,
            "insert_date": insert_date,
        }
        for name_text, name_href, status, charity_type, province, city, status_date in zip(
            master_charity_name,
            master_charity_url,
            master_charity_status,
            master_charity_type,
            master_charity_province,
            master_charity_city,
            master_charity_status_date,
        )
    ]

    # Export data to csv
    file_name = "charity-list"
    export_to_csv(file_name, documents)

    # Export data to mongodb
    collection_name = "charity_list"
    export_to_mongodb(MONGODB_CONNECTION, collection_name, documents)

    return 0


def load_fv_dir():
    """Get full view urls for each charity where status = registered"""
    collection_name = "charity_list"
    client, collection = connect_to_mongodb(MONGODB_CONNECTION, collection_name)

    # Query where status = 'Registered'
    query = {"charity_status": "Registered"}
    result = collection.find(query)

    # Loop through results
    full_view_dir = []

    for r in result:
        fv_urls = get_fv_urls(r)  # Extract all of a charity's FV urls

        full_view_dir.extend(fv_urls)  # Add charity's fv urls to master list

        random_sleep(1, 10)  # Pause so website doesn't crash

    # Export charity full view directory into csv and MongoDB
    file_name = "charity-fv-dir"
    export_to_csv(file_name, full_view_dir)

    # Export data to mongodb
    collection_name = "charity_fv_dir"
    export_to_mongodb(MONGODB_CONNECTION, collection_name, full_view_dir)

    client.close()

    return 0


def get_fv_urls(doc):
    """Return charity full view urls"""
    master_charity_name = []
    master_reg_num = []
    master_fv_url = []
    master_fv_year = []

    # Load registration numbers for a specific charity
    charity_name = doc.get("charity_name")
    charity_url = doc.get("charity_url")
    target_url = CRA_PARENT_URL + charity_url

    # Check if the key exists in the document
    if target_url is not None:
        # Load html data
        soup = load_html(target_url)

        # Get the charity's registration number
        target_reg_num = soup.find("div", class_="col-xs-12 col-sm-6 col-md-6 col-lg-9")
        reg_num_messy = target_reg_num.text.strip()
        parts = reg_num_messy.split("\r")
        reg_num = parts[0]

        # Get the charity's Full View urls, ensuring the preceding div is for Full View (vs Quick View)
        preceding_div = soup.find(
            lambda tag: tag.name == "div" and "Full View" in tag.get_text(strip=True),
            class_="h3 row mrgn-lft-sm mrgn-bttm-md",
        )  # Find the Full View child element

        parent_element = preceding_div.find_parent(
            "section"
        )  # Find the Full View parent element

        target_fv = parent_element.find(
            "ul", class_="list-unstyled mrgn-lft-md"
        )  # Find the ul element containing links

        target_fv_list = target_fv.find_all(
            "li"
        )  # Find all the sub elements containing FV urls and submission years

        master_fv_url = [
            fv.find("a")["href"].strip() for fv in target_fv_list if fv.find("a")
        ]
        master_fv_year = [
            fv.find("a").text.strip() for fv in target_fv_list if fv.find("a")
        ]

    else:
        charity_name = doc.get("charity_name", None)
        print(f"{charity_name}'s href isn't working.")

    # Create table of reg_num, urls, and years
    max_length = max(len(master_fv_url), len(master_fv_year))
    master_charity_name = [charity_name] * max_length
    master_reg_num = [reg_num] * max_length

    # Get insert date
    insert_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Combine lists into a list of dictionaries
    document = [
        {
            "charity_name": name,
            "charity_url": rn,
            "fv_url": url,
            "fv_year": yr,
            "insert_date": insert_date,
        }
        for name, rn, url, yr in zip(
            master_charity_name,
            master_reg_num,
            master_fv_url,
            master_fv_year,
        )
    ]

    return document


def load_fv_detail():
    """
    Load detailed data for each charity fv url, including:
    - Directors information
    - Financial information (Section D or Schedule 6, whichever is available)
    - Qualified donees inforamtion (if available)
    """

    # For each charity fv, extract details
    collection_name = "charity_fv_dir"
    client, collection = connect_to_mongodb(MONGODB_CONNECTION, collection_name)

    # Extract latest documents - assuming no older than a week before the latest fv value
    most_recent_insert_date = collection.find_one(
        {}, {"_id": 0, "insert_date": 1}, sort=[("insert_date", -1)]
    )[
        "insert_date"
    ]  # Get most recent insert_date

    return 0


def load_directors():
    return 0


def load_section_d():
    return 0


def load_schedule_6():
    return 0


def load_donees():
    return 0


def main():
    """
    1. Load urls to all charities
    2. Load full view urls for all registered charities. For each charity, get fv urls
    3. Load charity details for all fv urls. For each fv url, get: director info, section D financials (if avail.), schedule 6 financials (if avail.), and list of donees (if avail.)
    """
    load_charity_list()
    load_fv_dir()
    load_fv_detail()


### Run the scraper
main()
