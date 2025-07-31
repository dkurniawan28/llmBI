#!/usr/bin/env python3
"""
Find Transaction Data - Look for collections containing actual transaction data
"""

# Load environment variables first
import load_env

from mongodb_connection import MongoDBSSHConnection
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def find_transaction_collections():
    """Find collections that contain transaction data"""
    logger.info("🔍 Looking for collections with transaction data...")
    
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    
    if not client:
        logger.error("❌ Failed to connect to MongoDB")
        return False
    
    db = mongo_conn.get_database()
    
    # List all collections
    collections = db.list_collection_names()
    print(f"📂 Found {len(collections)} collections in database:")
    
    for i, coll_name in enumerate(collections, 1):
        if coll_name.strip():  # Skip empty collection names
            collection = db[coll_name]
            count = collection.count_documents({})
            print(f"  {i:2d}. {coll_name}: {count:,} documents")
    
    # Look for collections that might contain transaction data
    transaction_candidates = []
    for coll_name in collections:
        if coll_name.strip() and any(term in coll_name.lower() for term in ['transaction', 'sales', 'sale', 'payment', 'order']):
            transaction_candidates.append(coll_name)
    
    print(f"\n💳 POTENTIAL TRANSACTION COLLECTIONS:")
    if transaction_candidates:
        for coll_name in transaction_candidates:
            collection = db[coll_name]
            count = collection.count_documents({})
            print(f"  ✅ {coll_name}: {count:,} documents")
    else:
        print("  ❌ No obvious transaction collections found by name")
    
    # Check collections with significant data
    print(f"\n📊 COLLECTIONS WITH DATA (>0 documents):")
    non_empty_collections = []
    for coll_name in collections:
        if coll_name.strip():  # Skip empty collection names
            collection = db[coll_name]
            count = collection.count_documents({})
            if count > 0:
                non_empty_collections.append((coll_name, count))
    
    # Sort by document count
    non_empty_collections.sort(key=lambda x: x[1], reverse=True)
    
    for coll_name, count in non_empty_collections:
        print(f"  📦 {coll_name}: {count:,} documents")
        
        # Sample first document to check structure
        collection = db[coll_name]
        sample = collection.find_one()
        if sample:
            # Look for transaction-related fields
            fields = list(sample.keys())
            transaction_fields = []
            for field in fields:
                if any(term in field.lower() for term in ['date', 'sales', 'payment', 'total', 'amount', 'price', 'method', 'transaction']):
                    transaction_fields.append(field)
            
            if transaction_fields:
                print(f"    🔍 Transaction-related fields: {transaction_fields}")
                # Show sample values
                for field in transaction_fields[:5]:  # Show first 5 fields
                    if field in sample:
                        print(f"      {field}: {sample[field]} ({type(sample[field]).__name__})")
                print("    " + "-" * 50)
    
    mongo_conn.disconnect()
    return True

if __name__ == "__main__":
    print("🔍 Finding Transaction Data Collections")
    print("=" * 60)
    find_transaction_collections()