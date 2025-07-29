#!/usr/bin/env python3

import os
from mongodb_connection import MongoDBSSHConnection

def test_transaction_sale_functionality():
    """
    Test script for transaction_sale collection with OpenRouter Claude integration
    """
    
    # Check if API key is set
    if not os.getenv('OPENROUTER_API_KEY'):
        print("❌ OPENROUTER_API_KEY environment variable not set!")
        print("Please set it with: export OPENROUTER_API_KEY='your_api_key_here'")
        return False
    
    mongo_conn = MongoDBSSHConnection()
    
    try:
        print("🔌 Connecting to MongoDB...")
        client = mongo_conn.connect()
        
        if not client:
            print("❌ Failed to connect to MongoDB")
            return False
        
        print("✅ Connected successfully!")
        
        # Test 1: Generate sample data
        print("\n🤖 Generating sample data with Claude...")
        sample_data = mongo_conn.generate_sample_data_with_claude(3)
        
        if not sample_data:
            print("❌ Failed to generate sample data")
            return False
        
        print(f"✅ Generated {len(sample_data)} sample records")
        
        # Test 2: Insert data
        print("\n💾 Inserting data to MongoDB...")
        result = mongo_conn.insert_transaction_data(sample_data)
        print("✅ Data inserted successfully!")
        
        # Test 3: Run aggregations
        print("\n📊 Running aggregation pipelines...")
        
        # Available pipelines
        pipelines = [
            "sales_by_location",
            "payment_methods", 
            "product_performance",
            "daily_sales",
            "hourly_pattern"
        ]
        
        for pipeline in pipelines:
            try:
                result = mongo_conn.run_aggregation(pipeline)
                print(f"✅ {pipeline}: {len(result)} results")
                
                # Show sample of first result
                if result:
                    print(f"   Sample: {result[0]}")
                    
            except Exception as e:
                print(f"❌ {pipeline}: {e}")
        
        # Test 4: Check total records
        db = mongo_conn.get_database()
        collection = db['transaction_sale']
        total = collection.count_documents({})
        print(f"\n📈 Total records in collection: {total}")
        
        print("\n🎉 All tests completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        return False
        
    finally:
        mongo_conn.disconnect()
        print("🔌 Disconnected from MongoDB")

if __name__ == "__main__":
    print("🧪 Testing Transaction Sale Functionality")
    print("=" * 50)
    
    success = test_transaction_sale_functionality()
    
    if success:
        print("\n✅ All tests passed!")
    else:
        print("\n❌ Some tests failed!")