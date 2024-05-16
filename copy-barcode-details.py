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
products = db['products'].find({}, {
    'code': 1,
    '_id': 1,
    'product_name': 1,
    'product_name_en': 1,
    'ingredients_text': 1,
    'ingredients_text_en': 1,
    'allergens_imported': 1,
    'categories_old': 1,
    'categories_tags': 1,
    'lang': 1,
    'brands': 1,
    'brand_owner': 1,
    'link': 1,
})

length = products.count()
print ("collections items:", length, "\n")

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
    'link',
])

count = 0
for document in products.find({}, ):
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
        document.get('link', ''),
    ])

    # print("{}. writing {}".format(count, code))
    common.printProgressBar(count, prefix = 'Progress:', suffix = 'Complete', decimals=4, length = 100, total=length)

