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

barcode_file_name = 'barcode-database-reverse.csv'

with open(barcode_file_name, 'r') as f:
    length = sum(1 for row in f)

with open(barcode_file_name, 'r') as file:
    CsvReader = csv.reader(file)

    count = 0
    print(f"Uploading {length} of records to the {col_name}...")
    for line in CsvReader:
        count += 1
        if count == 1:
            continue
        
        if len(line) < 7:
            print(' ------- XX {} XX ------- '.format(len(line)))
            continue

        record = {
            'code': line[0],
            'title': line[1] or line[2],
            'lang': line[4],
            'brand': line[5],
            'manufacturer': line[6],
            'category': line[3],
        }

        try:
            # barcodes.insert_one(record)
            result = barcodes.update_one(
                {'id': record['code']},
                {'$set': record},
                # upsert=True,
            )
            # print(f'{count}. {result.matched_count}\t{result.modified_count}\t{line[1]}\t{line[2]}')
            # print(f'.', end="")
            # common.printProgressBar(count, prefix = 'Progress:', suffix = 'Complete', decimals=4, length = 100, total=length)
            common.printProgressBar(count, prefix = f'{count:,}/{length:,}', suffix = 'Complete', decimals=3, length = 100, total=length)
        except Exception as e:
            print(f'Error: {e}')
        
    print("Done!")