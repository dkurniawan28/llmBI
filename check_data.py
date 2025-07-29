#!/usr/bin/env python3

from mongodb_connection import MongoDBSSHConnection

def check_actual_data():
    print("🔍 Checking actual data in collection...")
    
    mongo_conn = MongoDBSSHConnection()
    
    try:
        client = mongo_conn.connect()
        db = mongo_conn.get_database()
        collection = db['transaction_sale']
        
        # Check total documents
        total = collection.count_documents({})
        print(f"📊 Total documents: {total}")
        
        # Check sample dates
        print("\n📅 Sample Sales Dates:")
        sample_dates = list(collection.find({}, {"Sales Date": 1, "_id": 0}).limit(10))
        for i, doc in enumerate(sample_dates, 1):
            print(f"{i:2d}. {doc.get('Sales Date', 'N/A')}")
        
        # Check if month/year fields exist
        sample_with_month = collection.find_one({"month": {"$exists": True}})
        if sample_with_month:
            print(f"\n✅ Month field exists. Sample: month={sample_with_month.get('month')}, year={sample_with_month.get('year')}")
        else:
            print("\n❌ Month field doesn't exist yet")
        
        # Check what months are actually available
        month_pipeline = [
            {"$group": {"_id": "$month", "count": {"$sum": 1}, "sample_date": {"$first": "$Sales Date"}}},
            {"$sort": {"_id": 1}}
        ]
        
        months_available = list(collection.aggregate(month_pipeline))
        print(f"\n📈 Available months:")
        for month_data in months_available:
            month_num = month_data['_id']
            month_name = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            month_str = month_name[month_num] if month_num and 1 <= month_num <= 12 else "Unknown"
            print(f"   Month {month_num} ({month_str}): {month_data['count']} records - Sample date: {month_data['sample_date']}")
        
        # Check for June specifically
        june_count = collection.count_documents({"month": 6})
        print(f"\n🔍 June (month=6) records: {june_count}")
        
        if june_count == 0:
            print("❌ No June data found! This is why the aggregation returns 0 results.")
            
            # Check what we can find by date regex
            june_regex_count = collection.count_documents({"Sales Date": {"$regex": "^\\d{2}/06/\\d{4}$"}})
            print(f"🔍 June by regex (DD/06/YYYY): {june_regex_count}")
            
            if june_regex_count > 0:
                print("✅ June data exists but month field extraction failed!")
                sample_june = collection.find_one({"Sales Date": {"$regex": "^\\d{2}/06/\\d{4}$"}})
                print(f"   Sample June record: {sample_june.get('Sales Date')} -> month field: {sample_june.get('month', 'MISSING')}")
        
        mongo_conn.disconnect()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_actual_data()