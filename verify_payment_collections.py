#!/usr/bin/env python3
"""
Verify Payment Collections - Check that all collections are properly populated
"""

# Load environment variables first
import load_env

from mongodb_connection import MongoDBSSHConnection
import json

def verify_payment_collections():
    """Verify all payment collections have data"""
    
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    
    if not client:
        print("❌ Failed to connect to MongoDB")
        return False
    
    db = mongo_conn.get_database()
    
    collections_to_check = [
        'payment_by_week',
        'payment_by_day', 
        'payment_by_month'
    ]
    
    print(f"✅ PAYMENT COLLECTIONS VERIFICATION")
    print("=" * 50)
    
    for collection_name in collections_to_check:
        collection = db[collection_name]
        count = collection.count_documents({})
        
        print(f"\n📦 {collection_name}:")
        print(f"  📊 Document count: {count:,}")
        
        if count > 0:
            # Get unique payment methods
            payment_methods = collection.distinct("payment_method")
            print(f"  💳 Payment methods: {payment_methods}")
            
            # Show date range
            if collection_name == 'payment_by_week':
                date_range = list(collection.aggregate([
                    {"$group": {"_id": None, "min_date": {"$min": "$start_date"}, "max_date": {"$max": "$end_date"}}}
                ]))
            elif collection_name == 'payment_by_day':
                date_range = list(collection.aggregate([
                    {"$group": {"_id": None, "min_date": {"$min": "$date"}, "max_date": {"$max": "$date"}}}
                ]))
            else:  # monthly
                date_range = list(collection.aggregate([
                    {"$group": {"_id": None, "min_year": {"$min": "$year"}, "max_year": {"$max": "$year"}, 
                               "min_month": {"$min": "$month"}, "max_month": {"$max": "$month"}}}
                ]))
            
            if date_range:
                print(f"  📅 Date range: {date_range[0]}")
            
            # Show sample document
            sample = collection.find_one()
            print(f"  📄 Sample document fields: {list(sample.keys())}")
            
        else:
            print(f"  ❌ Collection is empty!")
    
    print(f"\n🎉 Verification complete!")
    mongo_conn.disconnect()
    return True

if __name__ == "__main__":
    verify_payment_collections()