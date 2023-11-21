# This file runs when the Python module is executed with the '-m' flag, such as `python -m charity_data`

# Setup environment
import requests
import pymongo
import certifi
import time
import csv

from bs4 import BeautifulSoup
from pymongo import MongoClient
from certifi import where
from datetime import datetime


### Global variables
mongodb_username = "danyangyan"
mongodb_password = "5JhJMQ2itEnVW2Wm"
mongodb_connection = (
    "mongodb+srv://"
    + mongodb_username
    + ":"
    + mongodb_password
    + "@discourse.mwgzz8m.mongodb.net"
)


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
    # Connect to MongoDB
    client = MongoClient(connection, tlsCAFile=certifi.where())

    # Select the database (create one if it doesn't exist)
    db = client["charity_data"]

    # Select or create a collection
    collection = db[collection_name]

    # Insert the list of documents into the collection
    collection.insert_many(input_data)
    """
    # Insert the list of documents into the collection and print the inserted document IDs
    result = collection.insert_many(input_data)

    # Print the inserted document IDs
    print("Inserted document IDs:", result.inserted_ids)
    """

    # Close the connection
    client.close()


def load_charity_list():
    """Load list of all charities."""
    # Set up source and lists to receive load
    target_url = "https://apps.cra-arc.gc.ca/ebci/hacc/srch/pub/bscSrch"
    page_num = 1

    master_charity_org_name_text = []
    master_charity_org_name_href = []
    master_charity_status = []
    master_charity_type = []
    master_charity_province = []
    master_charity_city = []
    master_charity_status_date = []

    # Load html data
    soup = load_html(target_url)

    # While a next page exists, append table data to master lists
    while soup != -1:
        ### Gather output table data including charity name, url, status, etc.
        # Find all td elements with headers="headername", "header2" through "header6"
        headername_tds = soup.find_all("td", {"headers": "headername"})
        header2_tds = soup.find_all("td", {"headers": "header2"})
        header3_tds = soup.find_all("td", {"headers": "header3"})
        header4_tds = soup.find_all("td", {"headers": "header4"})
        header5_tds = soup.find_all("td", {"headers": "header5"})
        header6_tds = soup.find_all("td", {"headers": "header6"})

        # Extract text and href from each <a> element and store in lists
        charity_org_name_text = [
            td.find("a").text.strip() for td in headername_tds if td.find("a")
        ]
        charity_org_name_href = [
            td.find("a")["href"].strip() for td in headername_tds if td.find("a")
        ]

        # Extract text from each td element and store in lists
        charity_status = [td.text.strip() for td in header2_tds]
        charity_type = [td.text.strip() for td in header3_tds]
        charity_province = [td.text.strip() for td in header4_tds]
        charity_city = [td.text.strip() for td in header5_tds]
        charity_status_date = [td.text.strip() for td in header6_tds]

        # Append lists to master lists
        master_charity_org_name_text.extend(charity_org_name_text)
        master_charity_org_name_href.extend(charity_org_name_href)
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
        time.sleep(5)

    # Combine lists into a list of dictionaries, adding insert date
    insert_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    documents = [
        {
            "charity_org_name_text": name_text,
            "charity_org_name_href": name_href,
            "charity_status": status,
            "charity_type": charity_type,
            "charity_province": province,
            "charity_city": city,
            "charity_status_date": status_date,
            "insert_date": insert_date,
        }
        for name_text, name_href, status, charity_type, province, city, status_date in zip(
            master_charity_org_name_text,
            master_charity_org_name_href,
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
    export_to_mongodb(mongodb_connection, collection_name, documents)


def load_charity_data(charity_url):
    """Load charity data including registration number and pointers to the last 5 years of detailed information."""
    # Load registration numbers for a specific charity

    # Load URLs for the last 5 years of full view data

    return 0


def load_dir_info(charity_year_url):
    """For a charity's specified year, load the director and trustees information."""
    return 0


def load_fin_position(charity_year_url):
    """For a charity's specified year, load the statement of financial position."""
    return 0


def load_ops_revenues(charity_year_url):
    """For a charity's specified year, load the statement of operations, revenues."""
    return 0


def load_ops_expenses(charity_year_url):
    """For a charity's specified year, load the statement of operations, expenses."""
    return 0


def load_quota(charity_year_url):
    """For a charity's specified year, load other financial information, permission to reduce disbursement quota."""
    return 0


def main():
    """
    Run the scraper:

    Get list of charities
    For each charity where status = registered:
        Add registration number to each charity
        Get last 5 years detailed data url

        For each year get:
            director info,
            statement of fin. position,
            statement of ops. - rev,
            statement of ops. - exp,
            other fin. info - perm. reduce disbursement quota
    """

    # Load all charity data
    load_charity_list()

    # For each charity where status = registered, get it's registration number and pointers to last 5 years of detailed data

    # For each charity's year of detailed data, get detailed information
    # load_dir_info()
    # load_fin_position()
    # load_ops_revenues()
    # load_ops_expenses()
    # load_quota()


### RUN THE SCRAPER
main()
