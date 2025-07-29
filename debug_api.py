#!/usr/bin/env python3

import os
import json
from mongodb_connection import MongoDBSSHConnection

def debug_api_components():
    """Debug each component of the API separately"""
    
    print("🔍 Debugging API Components")
    print("=" * 50)
    
    # Test 1: Check environment variable
    print("1. Checking OPENROUTER_API_KEY...")
    api_key = os.getenv('OPENROUTER_API_KEY')
    if api_key:
        print(f"✅ API key found: {api_key[:10]}...")
    else:
        print("❌ API key not found!")
        return
    
    # Test 2: MongoDB connection
    print("\n2. Testing MongoDB connection...")
    try:
        mongo_conn = MongoDBSSHConnection()
        client = mongo_conn.connect()
        if client:
            print("✅ MongoDB connected successfully")
            
            # Check if collection exists and has data
            db = mongo_conn.get_database()
            collection = db['transaction_sale']
            count = collection.count_documents({})
            print(f"📊 Collection 'transaction_sale' has {count} documents")
            
            if count == 0:
                print("⚠️  Collection is empty! Need to add sample data first.")
                
                # Generate and insert sample data
                print("🤖 Generating sample data...")
                sample_data = mongo_conn.generate_sample_data_with_claude(3)
                if sample_data:
                    result = mongo_conn.insert_transaction_data(sample_data)
                    print(f"✅ Inserted {len(sample_data)} sample records")
                else:
                    print("❌ Failed to generate sample data")
            
        else:
            print("❌ MongoDB connection failed")
            return
    except Exception as e:
        print(f"❌ MongoDB error: {e}")
        return
    
    # Test 3: Test aggregation
    print("\n3. Testing aggregation pipeline...")
    try:
        # Simple aggregation test
        pipeline = [
            {"$group": {"_id": "$Location Name", "total": {"$sum": 1}}},
            {"$sort": {"total": -1}},
            {"$limit": 5}
        ]
        
        results = list(collection.aggregate(pipeline))
        print(f"✅ Aggregation successful: {len(results)} results")
        for result in results:
            print(f"   {result}")
            
    except Exception as e:
        print(f"❌ Aggregation error: {e}")
    
    # Test 4: Test AI services (if data exists)
    if count > 0:
        print("\n4. Testing AI services...")
        try:
            from api_server import AIService
            ai_service = AIService()
            
            # Test translation
            test_command = "tampilkan penjualan per lokasi bulan juni"
            translated = ai_service.translate_with_mixtral(test_command)
            print(f"✅ Translation: '{test_command}' -> '{translated}'")
            
            # Test pipeline generation
            with open('support/transaction_sale.json', 'r') as f:
                schema = json.load(f)
            
            pipeline = ai_service.generate_pipeline_with_claude(translated, schema)
            print(f"✅ Generated pipeline: {json.dumps(pipeline, indent=2)}")
            
            # Test the generated pipeline
            results = list(collection.aggregate(pipeline))
            print(f"✅ Pipeline execution: {len(results)} results")
            
        except Exception as e:
            print(f"❌ AI services error: {e}")
            import traceback
            traceback.print_exc()
    
    mongo_conn.disconnect()
    print("\n🎉 Debug complete!")

if __name__ == "__main__":
    debug_api_components()