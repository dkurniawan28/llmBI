#!/usr/bin/env python3

from mongodb_connection import MongoDBSSHConnection
import json

def test_june_query():
    print("🧪 Testing June query directly...")
    
    mongo_conn = MongoDBSSHConnection()
    
    try:
        client = mongo_conn.connect()
        db = mongo_conn.get_database()
        collection = db['transaction_sale']
        
        # Test simple June query
        june_pipeline = [
            {"$match": {"month": 6}},
            {"$group": {"_id": "$Location Name", "total_sales": {"$sum": {"$toDouble": "$Total"}}, "count": {"$sum": 1}}},
            {"$sort": {"total_sales": -1}},
            {"$limit": 10}
        ]
        
        print(f"Pipeline: {json.dumps(june_pipeline, indent=2)}")
        
        results = list(collection.aggregate(june_pipeline))
        
        print(f"\n📊 Results: {len(results)} records")
        for i, result in enumerate(results, 1):
            print(f"{i}. {result}")
        
        # Test the pipeline that Claude might generate
        claude_style_pipeline = [
            {"$match": {"month": 6, "year": 2024}},
            {"$group": {
                "_id": {"location": "$Location Name", "month": "$month"}, 
                "total_sales": {"$sum": {"$toDouble": "$Total"}}, 
                "count": {"$sum": 1}
            }},
            {"$project": {
                "_id": 0,
                "location": "$_id.location",
                "month": "$_id.month", 
                "total_sales": {"$round": ["$total_sales", 2]},
                "transactions": "$count"
            }},
            {"$sort": {"total_sales": -1}}
        ]
        
        print(f"\n🤖 Claude-style pipeline:")
        print(json.dumps(claude_style_pipeline, indent=2))
        
        claude_results = list(collection.aggregate(claude_style_pipeline))
        print(f"\n📊 Claude-style Results: {len(claude_results)} records")
        for i, result in enumerate(claude_results, 1):
            print(f"{i}. {result}")
        
        mongo_conn.disconnect()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_june_query()