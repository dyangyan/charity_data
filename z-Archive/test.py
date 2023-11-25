import certifi
from pymongo import MongoClient
from certifi import where

# Connect to MongoDB
mongodb_username = "danyangyan"
mongodb_password = "5JhJMQ2itEnVW2Wm"
mongodb_connection = (
    "mongodb+srv://"
    + mongodb_username
    + ":"
    + mongodb_password
    + "@discourse.mwgzz8m.mongodb.net"
)

client = MongoClient(mongodb_connection, tlsCAFile=certifi.where())
database = client["charity_data"]
collection = database["charity_fv_dir"]

# Aggregate pipeline to find unique field3 values for each unique field1 and field2 pair
pipeline = [
    {"$unwind": {"path": "$charity_org_name_text", "includeArrayIndex": "idx"}},
    {
        "$group": {
            "_id": {
                "charity_org_name_text": "$charity_org_name_text",
                "insert_date": "$insert_date",
            },
            "charity_reg_num": {"$addToSet": "$charity_reg_num"},
            "full_view_url": {"$addToSet": "$full_view_url"},
            "full_view_year": {"$addToSet": "$full_view_year"},
        }
    },
    {"$unwind": "$charity_reg_num_values"},
    {"$unwind": "$full_view_url_values"},
    {"$unwind": "$full_view_year_values"},
    {
        "$project": {
            "_id": 0,
            "charity_org_name_text": "$_id.charity_org_name_text",
            "insert_date": "$_id.insert_date",
            "charity_reg_num": "$charity_reg_num_values",
            "full_view_url": "$full_view_url_values",
            "full_view_year": "$full_view_year_values",
        }
    },
]

# Execute the aggregation pipeline
result = list(collection.aggregate(pipeline))

# Print or process the result
print(result)
