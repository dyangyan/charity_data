import requests
import pymongo
import certifi
import time
import csv

from bs4 import BeautifulSoup
from pymongo import MongoClient
from certifi import where
from datetime import datetime

target_url = "https://apps.cra-arc.gc.ca/ebci/hacc/srch/pub/bscSrch"
page_num = 1

master_pagination = []
master_charity_org_name_text = []
master_charity_org_name_href = []
master_charity_status = []
master_charity_type = []
master_charity_province = []
master_charity_city = []
master_charity_status_date = []


### Method for extracting html content from url
def get_html(target_url):
    try:
        html_content = requests.get(target_url)
        html_content.raise_for_status()
        output = BeautifulSoup(html_content.text, "html.parser")

        return output

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")

        return -1


### Set up beautifulsoup
soup = get_html(target_url)


### Gather pagination data from the current page
# Find the ul element with class "pagination"
pagination_ul = soup.find("ul", class_="pagination")

# Extract href values from all a elements within the ul
pagination_hrefs = [a.get("href", "").strip() for a in pagination_ul.find_all("a")]

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
    char_status_date = [td.text.strip() for td in header6_tds]

    # Append lists to master lists
    master_pagination.extend(pagination_hrefs)
    master_charity_org_name_text.extend(charity_org_name_text)
    master_charity_org_name_href.extend(charity_org_name_href)
    master_charity_status.extend(charity_status)
    master_charity_type.extend(charity_type)
    master_charity_province.extend(charity_province)
    master_charity_city.extend(charity_city)
    master_charity_status_date.extend(charity_status)

    # Increment pagination
    page_num = page_num + 1

    target_url = (
        "https://apps.cra-arc.gc.ca/ebci/hacc/srch/pub/bscSrch?dsrdPg="
        + str(page_num)
        + "&q.stts=0007&q.ordrClmn=NAME&q.ordrRnk=ASC"
    )

    soup = get_html(target_url)

    time.sleep(10)


### Load data to MongoDB
username = "danyangyan"
password = "5JhJMQ2itEnVW2Wm"

connection = (
    "mongodb+srv://" + username + ":" + password + "@discourse.mwgzz8m.mongodb.net"
)

# Combine lists into a list of dictionaries
documents = [
    {
        "charity_org_name_text": name_text,
        "charity_org_name_href": name_href,
        "charity_status": status,
        "charity_type": charity_type,
        "charity_province": province,
        "charity_city": city,
        "charity_status_date": status_date,
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

# Export dictionaries to csv output
today_date = datetime.today().strftime("%Y-%m-%d")
csv_file_path = (
    "/Users/danyan/Library/CloudStorage/OneDrive-Personal/1-Professional/2-Skills/vs-code-workspace/charity_data/"
    + today_date
    + "-export-cra-charity-list.csv"
)

with open(csv_file_path, "w", newline="") as csv_file:
    fieldnames = documents[0].keys()  # Assumes all dictionaries have the same keys
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    # Write the header
    writer.writeheader()

    # Write the data
    writer.writerows(documents)

# Connect to MongoDB
client = MongoClient(connection, tlsCAFile=certifi.where())

# Select the database (create one if it doesn't exist)
db = client["charity_data"]

# Select or create a collection
collection = db["charity_list"]

# Insert the list of documents into the collection
result = collection.insert_many(documents)

# Print the inserted document IDs
print("Inserted document IDs:", result.inserted_ids)

# Close the connection
client.close()
