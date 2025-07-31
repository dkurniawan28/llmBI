#!/usr/bin/env python3
"""
Robust Collection Check - Find transaction data safely
"""

# Load environment variables first
import load_env

from mongodb_connection import MongoDBSSHConnection
import json

def robust_check():
    """Robust check for collections"""
    
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    
    if not client:
        print("❌ Failed to connect to MongoDB")
        return False
    
    db = mongo_conn.get_database()
    
    # Get all collection names and filter safely
    all_collections = db.list_collection_names()
    print(f"📂 Total collections returned: {len(all_collections)}")
    
    # Filter out problematic collection names
    valid_collections = []
    for name in all_collections:
        if name and isinstance(name, str) and name.strip() and len(name.strip()) > 0:
            valid_collections.append(name.strip())
    
    print(f"📂 Valid collections: {len(valid_collections)}")
    
    # Look for transaction-related collections
    print(f"\n🔍 Looking for transaction-related collections:")
    transaction_collections = []
    
    for coll_name in valid_collections:
        try:
            if 'transaction' in coll_name.lower():
                collection = db[coll_name]
                count = collection.count_documents({})
                transaction_collections.append((coll_name, count))
                print(f"  ✅ FOUND: {coll_name} - {count:,} documents")
        except Exception as e:
            print(f"  ❌ Error checking {coll_name}: {e}")
    
    # If no transaction collections found, look for other potential candidates
    if not transaction_collections:
        print(f"\n❌ No 'transaction' collections found. Checking other collections...")
        
        potential_collections = []
        for coll_name in valid_collections:
            try:
                # Skip obviously non-transaction collections
                if any(skip in coll_name.lower() for skip in ['log', 'jwt', 'upload', 'zip', 'language', 'task', 'message']):
                    continue
                
                collection = db[coll_name]
                count = collection.count_documents({})
                
                if count > 100:  # Only check collections with reasonable data
                    # Sample document to check for transaction-like fields
                    sample = collection.find_one()
                    if sample:
                        # Look for transaction indicators
                        transaction_indicators = 0
                        field_names = list(sample.keys())
                        
                        for field in field_names:
                            if any(term in field.lower() for term in ['payment', 'sales', 'total', 'receipt', 'price', 'date']):
                                transaction_indicators += 1
                        
                        if transaction_indicators >= 3:  # At least 3 transaction-like fields
                            potential_collections.append((coll_name, count, transaction_indicators))
                            print(f"  🔍 POTENTIAL: {coll_name} - {count:,} docs, {transaction_indicators} indicators")
            
            except Exception as e:
                print(f"  ⚠️ Error checking {coll_name}: {e}")
        
        # Use the most promising collection
        if potential_collections:
            # Sort by indicators first, then by count
            potential_collections.sort(key=lambda x: (x[2], x[1]), reverse=True)
            transaction_collections = [(potential_collections[0][0], potential_collections[0][1])]
    
    if transaction_collections:
        # Examine the best candidate
        main_collection_name, main_count = transaction_collections[0]
        print(f"\n🎯 Examining: {main_collection_name} ({main_count:,} documents)")
        
        try:
            collection = db[main_collection_name]
            sample = collection.find_one()
            
            if sample:
                print(f"\n📄 Key fields in sample document:")
                key_fields = []
                for field, value in sample.items():
                    if any(term in field.lower() for term in ['payment', 'sales', 'date', 'total', 'receipt']):
                        key_fields.append((field, value, type(value).__name__))
                        print(f"  📌 {field}: {value} ({type(value).__name__})")
                
                # Specifically check for Payment Method field
                print(f"\n💳 Payment Method Analysis:")
                if "Payment Method" in sample:
                    print(f"  ✅ 'Payment Method' field found: '{sample['Payment Method']}'")
                    
                    try:
                        payment_methods = collection.distinct("Payment Method")
                        print(f"  📊 Unique payment methods ({len(payment_methods)}):")
                        for method in payment_methods[:10]:  # Show first 10
                            if method is not None:
                                count = collection.count_documents({"Payment Method": method})
                                print(f"    - '{method}': {count:,}")
                    except Exception as e:
                        print(f"  ❌ Error getting payment methods: {e}")
                else:
                    print(f"  ❌ 'Payment Method' field not found")
                
                # Check Sales Date field
                print(f"\n📅 Sales Date Analysis:")
                if "Sales Date" in sample:
                    print(f"  ✅ 'Sales Date' field found: '{sample['Sales Date']}' ({type(sample['Sales Date']).__name__})")
                    
                    try:
                        # Test conditions
                        total_count = collection.count_documents({})
                        sales_date_not_none = collection.count_documents({"Sales Date": {"$ne": None}})
                        payment_not_none = collection.count_documents({"Payment Method": {"$ne": None}})
                        both_not_none = collection.count_documents({
                            "Sales Date": {"$ne": None},
                            "Payment Method": {"$ne": None}
                        })
                        
                        print(f"  📊 Total documents: {total_count:,}")
                        print(f"  📊 Sales Date not None: {sales_date_not_none:,}")
                        print(f"  📊 Payment Method not None: {payment_not_none:,}")
                        print(f"  📊 Both not None: {both_not_none:,}")
                        
                        if both_not_none == 0:
                            print(f"  🚨 ISSUE FOUND: No documents match both conditions!")
                            
                            # Check for null vs empty vs other values
                            sales_null = collection.count_documents({"Sales Date": None})
                            payment_null = collection.count_documents({"Payment Method": None})
                            
                            print(f"  🔍 Sales Date is null: {sales_null:,}")
                            print(f"  🔍 Payment Method is null: {payment_null:,}")
                            
                            # Check sample values
                            samples = list(collection.find().limit(5))
                            print(f"  📋 Sample values:")
                            for i, doc in enumerate(samples, 1):
                                sales_val = doc.get("Sales Date", "MISSING")
                                payment_val = doc.get("Payment Method", "MISSING")
                                print(f"    {i}. Sales Date: {sales_val} | Payment Method: {payment_val}")
                        
                    except Exception as e:
                        print(f"  ❌ Error analyzing dates: {e}")
                else:
                    print(f"  ❌ 'Sales Date' field not found")
                    
        except Exception as e:
            print(f"❌ Error examining collection: {e}")
    
    else:
        print(f"❌ No suitable transaction collections found!")
    
    mongo_conn.disconnect()
    return True

if __name__ == "__main__":
    print("🔍 Robust Collection Check")
    print("=" * 50)
    robust_check()