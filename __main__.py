# This file runs when the Python module is executed with the '-m' flag, such as `python -m charity_data`

# Setup environment
import requests
import pymongo
import certifi
import time
import csv

import get_charity_list

from bs4 import BeautifulSoup
from pymongo import MongoClient
from certifi import where
from datetime import datetime


### Global variables
target_url = None
page_num = None

mongodb_username = "danyangyan"
mongodb_password = "5JhJMQ2itEnVW2Wm"
mongodb_connection = (
    "mongodb+srv://"
    + mongodb_username
    + ":"
    + mongodb_password
    + "@discourse.mwgzz8m.mongodb.net"
)


# Runs the data pipeline
def main():
    """
    Get list of charities
    For each charity where status = registered:
        Add registration number to each charity
        Get last 5 years detailed data url

        For each year
            Get:
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
    load_dir_info()
    load_fin_position()
    load_ops_revenues()
    load_ops_expenses()
    load_quota()


# Extract html from url
def get_html(target_url):
    try:
        html_content = requests.get(target_url)
        html_content.raise_for_status()
        output = BeautifulSoup(html_content.text, "html.parser")

        return output

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")

        return -1


# Export dictionaries to CSV
def export_to_csv(file_name, input_data):
    todays_date = datetime.today().strftime("%Y-%m-%d")
    file_path = (
        "/Users/danyan/Library/CloudStorage/OneDrive-Personal/1-Education/code-workspace/charity_data/exports/"
        + todays_date
        + "-"
        + file_name
        + ".csv"
    )

    with open(file_path, "w", newline="") as csv_file:
        fieldnames = input_data[0].keys()  # Assumes all dictionaries have the same keys
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        # Write the header
        writer.writeheader()

        # Write the data
        writer.writerows(input_data)


# Export data to MongoDB
def export_to_mongodb(connection, collection_name, input_data):
    # Connect to MongoDB
    client = MongoClient(connection, tlsCAFile=certifi.where())

    # Select the database (create one if it doesn't exist)
    db = client["charity_data"]

    # Select or create a collection
    collection = db[collection_name]

    # Insert the list of documents into the collection
    result = collection.insert_many(input_data)

    # Print the inserted document IDs
    print("Inserted document IDs:", result.inserted_ids)

    # Close the connection
    client.close()


# Load list of all charities
def load_charity_list():
    return 0


# Load charity data including registration number and pointers to the last 5 years of detailed information
def load_charity_data(charity_url):
    # Load registration numbers for a specific charity

    # Load URLs for the last 5 years of full view data

    return 0


# For a charity's specified year, load the director and trustees information
def load_dir_info(charity_year_url):
    return 0


# For a charity's specified year, load the statement of financial position
def load_fin_position(charity_year_url):
    return 0


# For a charity's specified year, load the statement of operations, revenues
def load_ops_revenues(charity_year_url):
    return 0


# For a charity's specified year, load the statement of operations, expenses
def load_ops_expenses(charity_year_url):
    return 0


# For a charity's specified year, load other financial information, permission to reduce disbursement quota
def load_quota(charity_year_url):
    return 0
