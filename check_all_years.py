#!/usr/bin/env python3

from mongodb_connection import MongoDBSSHConnection

def check_all_years():
    print("🔍 Checking ALL years in collection...")
    
    mongo_conn = MongoDBSSHConnection()
    
    try:
        client = mongo_conn.connect()
        db = mongo_conn.get_database()
        collection = db['transaction_sale']
        
        # Check all unique years
        year_pipeline = [
            {"$group": {"_id": "$year", "count": {"$sum": 1}, "sample_dates": {"$push": "$Sales Date"}}},
            {"$sort": {"_id": 1}}
        ]
        
        years_available = list(collection.aggregate(year_pipeline))
        
        print(f"📊 Total documents: {collection.count_documents({})}")
        print(f"\n📅 Available years:")
        
        for year_data in years_available:
            year = year_data['_id']
            count = year_data['count']
            sample_dates = year_data['sample_dates'][:5]  # First 5 dates as sample
            
            print(f"   Year {year}: {count} records")
            print(f"      Sample dates: {sample_dates}")
            
        # Check for 2025 specifically
        count_2025 = collection.count_documents({"year": 2025})
        print(f"\n🔍 2025 records: {count_2025}")
        
        if count_2025 > 0:
            sample_2025 = list(collection.find({"year": 2025}, {"Sales Date": 1, "Location Name": 1, "Total": 1}).limit(3))
            print("✅ 2025 data found:")
            for doc in sample_2025:
                print(f"   {doc}")
        else:
            print("❌ No 2025 data found")
            
            # Check by date regex for 2025
            count_2025_regex = collection.count_documents({"Sales Date": {"$regex": "/2025$"}})
            print(f"🔍 2025 by regex: {count_2025_regex}")
            
            if count_2025_regex > 0:
                print("✅ 2025 data exists in Sales Date but year field might be wrong!")
        
        mongo_conn.disconnect()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_all_years()