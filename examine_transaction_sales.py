#!/usr/bin/env python3
"""
Examine Transaction Sales Collection - Check the correct collection name and structure
"""

# Load environment variables first
import load_env

from mongodb_connection import MongoDBSSHConnection
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def examine_transaction_sales():
    """Examine the transaction_sales collection (plural)"""
    logger.info("🔍 Examining transaction_sales collection (plural)...")
    
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    
    if not client:
        logger.error("❌ Failed to connect to MongoDB")
        return False
    
    db = mongo_conn.get_database()
    
    # Check if transaction_sales (plural) exists
    collections = db.list_collection_names()
    print(f"📂 Looking for transaction collections...")
    
    transaction_collections = []
    for coll_name in collections:
        if 'transaction' in coll_name.lower():
            collection = db[coll_name]
            count = collection.count_documents({})
            transaction_collections.append((coll_name, count))
    
    if transaction_collections:
        print(f"✅ Found transaction collections:")
        for coll_name, count in transaction_collections:
            print(f"  - {coll_name}: {count:,} documents")
    else:
        print(f"❌ No transaction collections found!")
        mongo_conn.disconnect()
        return False
    
    # Use the collection with the most documents (likely the main one)
    main_collection_name = max(transaction_collections, key=lambda x: x[1])[0]
    print(f"\n🎯 Using main collection: {main_collection_name}")
    
    collection = db[main_collection_name]
    
    # Sample documents to examine structure
    print(f"\n📄 SAMPLE DOCUMENT STRUCTURE:")
    print("="*80)
    
    sample = collection.find_one()
    if sample:
        print(json.dumps(sample, indent=2, default=str))
        
        # Extract field names
        field_names = list(sample.keys())
        print(f"\n🔑 FIELD NAMES ({len(field_names)} total):")
        for i, field in enumerate(field_names, 1):
            value = sample[field]
            print(f"  {i:2d}. '{field}' = {value} ({type(value).__name__})")
        
        # Check Payment Method field specifically
        print(f"\n💳 PAYMENT METHOD ANALYSIS:")
        if "Payment Method" in sample:
            print(f"  ✅ 'Payment Method' field exists: '{sample['Payment Method']}'")
            
            # Get unique payment methods
            payment_methods = collection.distinct("Payment Method")
            print(f"  📊 Found {len(payment_methods)} unique payment methods:")
            for i, method in enumerate(payment_methods, 1):
                count = collection.count_documents({"Payment Method": method})
                print(f"    {i:2d}. '{method}': {count:,} documents")
        else:
            print(f"  ❌ 'Payment Method' field not found!")
        
        # Check Sales Date field
        print(f"\n📅 SALES DATE ANALYSIS:")
        if "Sales Date" in sample:
            print(f"  ✅ 'Sales Date' field exists: '{sample['Sales Date']}' ({type(sample['Sales Date']).__name__})")
            
            # Check date formats
            sales_date_non_null = collection.count_documents({"Sales Date": {"$ne": None}})
            print(f"  📊 Documents with non-null Sales Date: {sales_date_non_null:,}")
        else:
            print(f"  ❌ 'Sales Date' field not found!")
        
        # Test the aggregation conditions
        print(f"\n🧪 TESTING AGGREGATION CONDITIONS:")
        
        # Current conditions from the pipeline
        both_conditions = collection.count_documents({
            "Sales Date": {"$ne": None},
            "Payment Method": {"$ne": None}
        })
        print(f"  📊 Documents matching both conditions: {both_conditions:,}")
        
        if both_conditions == 0:
            # Check for empty strings or other falsy values
            sales_date_exists = collection.count_documents({"Sales Date": {"$exists": True}})
            payment_exists = collection.count_documents({"Payment Method": {"$exists": True}})
            
            print(f"  🔍 Documents where 'Sales Date' exists: {sales_date_exists:,}")
            print(f"  🔍 Documents where 'Payment Method' exists: {payment_exists:,}")
            
            # Check for specific values
            sales_date_empty = collection.count_documents({"Sales Date": ""})
            payment_empty = collection.count_documents({"Payment Method": ""})
            
            print(f"  🔍 Documents with empty 'Sales Date': {sales_date_empty:,}")
            print(f"  🔍 Documents with empty 'Payment Method': {payment_empty:,}")
    
    mongo_conn.disconnect()
    return True

if __name__ == "__main__":
    print("🔍 Examining Transaction Sales Collection")
    print("=" * 60)
    examine_transaction_sales()