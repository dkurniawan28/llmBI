#!/usr/bin/env python3
"""
Simple Collection Check - Find the actual transaction data
"""

# Load environment variables first
import load_env

from mongodb_connection import MongoDBSSHConnection
import json

def simple_check():
    """Simple check for collections"""
    
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    
    if not client:
        print("❌ Failed to connect to MongoDB")
        return False
    
    db = mongo_conn.get_database()
    
    # Get all collection names and filter out empty ones
    all_collections = db.list_collection_names()
    collections = [name for name in all_collections if name and name.strip()]
    
    print(f"📂 Found {len(collections)} valid collections:")
    
    # Look specifically for transaction-related collections
    transaction_collections = []
    for coll_name in collections:
        if 'transaction' in coll_name.lower():
            collection = db[coll_name]
            count = collection.count_documents({})
            transaction_collections.append((coll_name, count))
            print(f"  ✅ TRANSACTION: {coll_name} - {count:,} documents")
    
    # If no transaction collections, look for other potential collections
    if not transaction_collections:
        print("❌ No 'transaction' collections found. Looking for other collections...")
        
        # Check collections with substantial data
        for coll_name in collections:
            collection = db[coll_name]
            count = collection.count_documents({})
            if count > 1000:  # Only show collections with substantial data
                print(f"  📦 {coll_name}: {count:,} documents")
                
                # Sample first document to look for transaction-like fields
                sample = collection.find_one()
                if sample:
                    # Look for payment/sales related fields
                    payment_fields = []
                    for field in sample.keys():
                        if any(term in field.lower() for term in ['payment', 'sales', 'total', 'receipt', 'date']):
                            payment_fields.append(field)
                    
                    if payment_fields:
                        print(f"    🔍 Has transaction-like fields: {payment_fields[:3]}...")
                        transaction_collections.append((coll_name, count))
    
    # Examine the most promising collection
    if transaction_collections:
        # Use the collection with the most documents
        main_collection_name, main_count = max(transaction_collections, key=lambda x: x[1])
        print(f"\n🎯 Examining main collection: {main_collection_name} ({main_count:,} docs)")
        
        collection = db[main_collection_name]
        sample = collection.find_one()
        
        if sample:
            print(f"\n📄 Sample document fields:")
            for field, value in sample.items():
                if 'payment' in field.lower() or 'sales' in field.lower() or 'date' in field.lower():
                    print(f"  ✅ {field}: {value} ({type(value).__name__})")
            
            # Check for Payment Method specifically
            if "Payment Method" in sample:
                payment_methods = collection.distinct("Payment Method")
                print(f"\n💳 Payment Methods ({len(payment_methods)} unique):")
                for method in payment_methods[:5]:  # Show first 5
                    count = collection.count_documents({"Payment Method": method})
                    print(f"  - '{method}': {count:,} docs")
            
            # Test the conditions from the aggregation
            print(f"\n🧪 Testing aggregation conditions:")
            total_docs = collection.count_documents({})
            
            if "Sales Date" in sample and "Payment Method" in sample:
                both_not_none = collection.count_documents({
                    "Sales Date": {"$ne": None},
                    "Payment Method": {"$ne": None}
                })
                print(f"  📊 Total docs: {total_docs:,}")
                print(f"  📊 Both Sales Date & Payment Method not None: {both_not_none:,}")
                
                if both_not_none == 0:
                    # Check what the actual values are
                    sales_date_null = collection.count_documents({"Sales Date": None})
                    payment_null = collection.count_documents({"Payment Method": None})
                    
                    print(f"  🔍 Sales Date is None: {sales_date_null:,}")
                    print(f"  🔍 Payment Method is None: {payment_null:,}")
                    
                    # Check for empty strings
                    sales_date_empty = collection.count_documents({"Sales Date": ""})
                    payment_empty = collection.count_documents({"Payment Method": ""})
                    
                    print(f"  🔍 Sales Date is empty string: {sales_date_empty:,}")
                    print(f"  🔍 Payment Method is empty string: {payment_empty:,}")
            
    mongo_conn.disconnect()
    return True

if __name__ == "__main__":
    print("🔍 Simple Collection Check")
    print("=" * 50)
    simple_check()