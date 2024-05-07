from CONSTANT import *
from ultimate3 import *
import pandas as pd


records = abs_df.to_dict(orient='index')

# # Authenticate with Firestore
# # Replace 'path/to/service_account_key.json' with the path to your service account key JSON file
# db = firestore.Client.from_service_account_json('path/to/service_account_key.json')

# # Create collections and insert records
# for row_id, record in records.items():
#     doc_ref = db.collection('your_collection').document(row_id)
#     doc_ref.set(record)

# print("Data inserted into Firestore successfully!")

# Function to print the structure of the dictionary
def print_dict_structure(data, prefix=''):
    for key, value in data.items():
        if isinstance(value, dict):
            print(f"{prefix}/{key}")
            print_dict_structure(value, prefix=f"{prefix}/{key}")
        else:
            print(f"{prefix}/{key}")

# Print the structure of the dictionary
print_dict_structure(records)