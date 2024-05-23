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

db = client.off

# collections = db.list_collection_names()

print("Counting documents in 'products' collection...")
length = db['products'].count_documents({})
print ("collections items:", length, "\n")

print('Getting products from the database...')
products = db['products'].find({}, {
    'code': 1,
    'product_name': 1,
    'product_name_en': 1,
    'categories_tags': 1,
    'lang': 1,
    'brands': 1,
    'brand_owner': 1,
}).sort('code', -1)

print('Writing to CSV file...')
CsvWriter = csv.writer(open('barcode-database-reverse.csv', 'w'))

print('Writing headers...')
CsvWriter.writerow([
    'code',
    'product_name',
    'product_name_en',
    'categories_tags',
    'lang',
    'brands',
    'brand_owner',
])

count = 0
for document in products:
    count += 1
    code = document.get('code', document.get('id', ''))

    CsvWriter.writerow([
        # document.get('_id', ''),
        code,
        document.get('product_name', ''),
        document.get('product_name_en', ''),
        ','.join(document.get('categories_tags', [])),
        document.get('lang', 'en'),
        document.get('brands', ''),
        document.get('brand_owner', ''),
    ])

    # print("{}. writing {}".format(count, code))
    # common.printProgressBar(count, prefix = 'Progress:', suffix = 'Complete', decimals=4, length = 100, total=length)
    common.printProgressBar(count, prefix = f'{count:,}/{length:,}', suffix = 'Complete', decimals=3, length = 100, total=length)

