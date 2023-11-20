import requests

url = "https://apps.cra-arc.gc.ca/ebci/hacc/srch/pub/bscSrchcom"

try:
    response = requests.get(url)
    response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx status codes)

    # Print the HTTP response code
    print(f"HTTP Response Code: {response.status_code}")

except requests.exceptions.RequestException as e:
    # Handle exceptions, such as connection errors or timeouts
    print(f"Error: {e}")
