#!/bin/python3

import os
import csv
from pymongo import MongoClient

MONGO_URL = os.environ['MONGO_URL']

mo_c = MongoClient(
    host="127.0.0.1",
    port=27017
)

db = mo_c.off

# collections = db.list_collection_names()
products = db['products']
print ("collections:", products, "\n")

CsvWriter = csv.writer(open('barcode-database.csv', 'w'))

CsvWriter.writerow([
    '_id',
    'code',
    'product_name',
    'product_name_en',
    'ingredients_text',
    'allergens_imported',
    'categories_old',
    'categories_tags',
    'lang',
    'brands',
    'brand_owner',
])

count = 0
for document in products.find():
    count += 1
    code = document.get('code', document.get('id', ''))

    CsvWriter.writerow([
        document.get('_id', ''),
        code,
        document.get('product_name', ''),
        document.get('product_name_en', ''),
        document.get('ingredients_text', document.get('ingredients_text_en', '')),
        document.get('allergens_imported', ''),
        document.get('categories_old', ''),
        ','.join(document.get('categories_tags', [])),
        document.get('lang', 'en'),
        document.get('brands', ''),
        document.get('brand_owner', ''),
    ])

    print("{}. writing {}".format(count, code))

