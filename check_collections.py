#!/usr/bin/env python3

import load_env
from mongodb_connection import MongoDBSSHConnection

mongo_conn = MongoDBSSHConnection()
client = mongo_conn.connect()
if client:
    db = mongo_conn.get_database()
    
    # Check sample documents from key collections
    collections_to_check = ['sales_by_location', 'sales_by_product', 'sales_by_location_month']
    
    for coll_name in collections_to_check:
        print(f'\n📋 {coll_name.upper()} - Sample Document:')
        sample = db[coll_name].find_one()
        if sample:
            for key, value in sample.items():
                if key != '_id':
                    print(f'  {key}: {value}')
        else:
            print('  No documents found')
    
    mongo_conn.disconnect()