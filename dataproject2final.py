#!/usr/bin/python3

import os
import json
from pymongo import MongoClient, errors

def load_json_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except json.JSONDecodeError as e:
        print(f"Corrupted JSON format in {file_path}: {e}")
        return None
    except Exception as e:
        print(f"Error when loading {file_path}: {e}")
        return None

def insert_data(collection, data):
    if data:
        if isinstance(data, list):
            try:
                result = collection.insert_many(data)
                print(f"Inserted {len(result.inserted_ids)} records from list.")
                return len(result.inserted_ids), 0 
            except Exception as e:
                print(f"Error when importing list into Mongo: {e}")
                return 0, len(data)  
        else:
            try:
                collection.insert_one(data)
                print("Inserted one record.")
                return 1, 0  
            except Exception as e:
                print(f"Error when importing single record into Mongo: {e}")
                return 0, 1  
    return 0, 0  

def main():
    MONGOPASS = os.getenv('MONGOPASS')
    uri = "mongodb+srv://jmh8fr.umgkslr.mongodb.net/"
    client = MongoClient(uri, username='jacobmhiggins', password='UyJsOH1gKgQttwNQ', connectTimeoutMS=200, retryWrites=True)
    db = client['jmh8fr']
    collection = db['hobbies']

    directory = 'data/'
    total_imported = 0
    total_failed = 0
    total_corrupted = 0

    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            data = load_json_file(file_path)
            if data is None:
                total_corrupted += 1
            else:
                imported, failed = insert_data(collection, data)
                total_imported += imported
                total_failed += failed

    print(f"Total Imported: {total_imported}")
    print(f"Total Failed: {total_failed}")
    print(f"Total Corrupted: {total_corrupted}")

    with open('count.txt', 'w') as file:
        file.write(f"Total Imported: {total_imported}\n")
        file.write(f"Total Failed: {total_failed}\n")
        file.write(f"Total Corrupted: {total_corrupted}\n")

if __name__ == '__main__':
    main()
