import csv

# Your list of dictionaries
data = [
    {"Name": "John", "Age": 25, "City": "New York"},
    {"Name": "Alice", "Age": 30, "City": "San Francisco"},
    {"Name": "Bob", "Age": 22, "City": "Chicago"},
]

# Specify the CSV file path
csv_file_path = "output.csv"

# Write the list of dictionaries to the CSV file
with open(csv_file_path, "w", newline="") as csv_file:
    fieldnames = data[0].keys()  # Assumes all dictionaries have the same keys
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    # Write the header
    writer.writeheader()

    # Write the data
    writer.writerows(data)

print(f"CSV file '{csv_file_path}' has been created.")
