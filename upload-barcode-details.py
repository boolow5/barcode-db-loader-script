#!/bin/python3

import os
import csv
from pymongo import MongoClient
import common

MONGO_URL = os.environ['MONGO_URL']

client = MongoClient(
    host="127.0.0.1",
    port=27017
)

db = client.get_database('bict-pos-api')
print('Database: {}'.format(db))
col_name = 'barcodes'
barcodes = db[col_name]
print('Collection: {}'.format(barcodes))
length = 0

with open('barcode-database.csv', 'r') as f:
    length = sum(1 for row in f)

with open('barcode-database.csv', 'r') as file:
    CsvReader = csv.reader(file)

    count = 0
    print(f"Uploading {length} of records to the {col_name}...")
    for line in CsvReader:
        count += 1
        if count == 1:
            continue
        
        if len(line) < 12:
            print(' ------- XX {} XX ------- '.format(len(line)))
            continue

        record = {
            '_id': line[0],
            'code': line[1],
            'title': line[2],
            'description': line[3] or line[4],
            # 'ingredients_text': line[4],
            # 'allergens_imported': line[5],
            # 'categories_old': line[6],
            # 'categories_tags': line[7],
            # 'lang': line[8],
            'brand': line[9],
            'manufacturer': line[10],
            'category': line[6] or line[7],
            # 'link': line[11],
        }


        try:
            # barcodes.insert_one(record)
            result = barcodes.update_one(
                {'id': record['_id']},
                {'$set': record},
                upsert=True,
            )
            # print(f'{count}. {result.matched_count}\t{result.modified_count}\t{line[1]}\t{line[2]}')
            # print(f'.', end="")
            common.printProgressBar(count, prefix = 'Progress:', suffix = 'Complete', decimals=4, length = 100, total=length)
        except Exception as e:
            print(f'Error: {e}')
        
    print("Done!")