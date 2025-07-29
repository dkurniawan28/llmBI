#!/usr/bin/env python3

from mongodb_connection import MongoDBSSHConnection
import json

def check_sales_date_aggregations():
    print("🔍 Testing aggregations on 'Sales Date' field...")
    
    mongo_conn = MongoDBSSHConnection()
    
    try:
        client = mongo_conn.connect()
        db = mongo_conn.get_database()
        collection = db['transaction_sale']
        
        print(f"📊 Total documents: {collection.count_documents({})}")
        
        # Test 1: Basic Sales Date grouping
        print("\n" + "="*60)
        print("TEST 1: Group by Sales Date")
        print("="*60)
        
        pipeline1 = [
            {"$group": {"_id": "$Sales Date", "count": {"$sum": 1}, "total_sales": {"$sum": {"$toDouble": "$Total"}}}},
            {"$sort": {"_id": 1}},
            {"$limit": 10}
        ]
        
        print(f"Pipeline: {json.dumps(pipeline1, indent=2)}")
        
        results1 = list(collection.aggregate(pipeline1))
        print(f"\nResults: {len(results1)} unique dates")
        for result in results1:
            print(f"  {result['_id']}: {result['count']} transactions, Rp {result['total_sales']:,.0f}")
        
        # Test 2: Extract year from Sales Date
        print("\n" + "="*60)
        print("TEST 2: Extract year from Sales Date")
        print("="*60)
        
        pipeline2 = [
            {
                "$addFields": {
                    "extracted_year": {
                        "$toInt": {
                            "$substr": ["$Sales Date", 6, 4]
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$extracted_year",
                    "count": {"$sum": 1},
                    "total_sales": {"$sum": {"$toDouble": "$Total"}},
                    "sample_dates": {"$push": "$Sales Date"}
                }
            },
            {"$sort": {"_id": 1}}
        ]
        
        print(f"Pipeline: {json.dumps(pipeline2, indent=2)}")
        
        results2 = list(collection.aggregate(pipeline2))
        print(f"\nResults: {len(results2)} unique years")
        for result in results2:
            sample_dates = result['sample_dates'][:5]  # First 5 as sample
            print(f"  Year {result['_id']}: {result['count']} transactions, Rp {result['total_sales']:,.0f}")
            print(f"    Sample dates: {sample_dates}")
        
        # Test 3: Extract month and year from Sales Date
        print("\n" + "="*60)
        print("TEST 3: Extract month and year from Sales Date")
        print("="*60)
        
        pipeline3 = [
            {
                "$addFields": {
                    "extracted_month": {
                        "$toInt": {
                            "$substr": ["$Sales Date", 3, 2]
                        }
                    },
                    "extracted_year": {
                        "$toInt": {
                            "$substr": ["$Sales Date", 6, 4]
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "year": "$extracted_year",
                        "month": "$extracted_month"
                    },
                    "count": {"$sum": 1},
                    "total_sales": {"$sum": {"$toDouble": "$Total"}},
                    "sample_dates": {"$push": "$Sales Date"}
                }
            },
            {"$sort": {"_id.year": 1, "_id.month": 1}}
        ]
        
        print(f"Pipeline: {json.dumps(pipeline3, indent=2)}")
        
        results3 = list(collection.aggregate(pipeline3))
        print(f"\nResults: {len(results3)} unique year-month combinations")
        for result in results3:
            year = result['_id']['year']
            month = result['_id']['month']
            count = result['count']
            sales = result['total_sales']
            sample_dates = result['sample_dates'][:3]
            
            month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            month_name = month_names[month] if 1 <= month <= 12 else "Invalid"
            
            print(f"  {year}-{month:02d} ({month_name}): {count} transactions, Rp {sales:,.0f}")
            print(f"    Sample dates: {sample_dates}")
        
        # Test 4: Sales by location and extracted year-month
        print("\n" + "="*60)
        print("TEST 4: Sales by location and extracted year-month")
        print("="*60)
        
        pipeline4 = [
            {
                "$addFields": {
                    "extracted_month": {
                        "$toInt": {
                            "$substr": ["$Sales Date", 3, 2]
                        }
                    },
                    "extracted_year": {
                        "$toInt": {
                            "$substr": ["$Sales Date", 6, 4]
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "location": "$Location Name",
                        "year": "$extracted_year",
                        "month": "$extracted_month"
                    },
                    "count": {"$sum": 1},
                    "total_sales": {"$sum": {"$toDouble": "$Total"}}
                }
            },
            {"$sort": {"_id.location": 1, "_id.year": 1, "_id.month": 1}},
            {"$limit": 20}
        ]
        
        print(f"Pipeline: {json.dumps(pipeline4, indent=2)}")
        
        results4 = list(collection.aggregate(pipeline4))
        print(f"\nResults: {len(results4)} location-year-month combinations (top 20)")
        for result in results4:
            location = result['_id']['location']
            year = result['_id']['year']
            month = result['_id']['month']
            count = result['count']
            sales = result['total_sales']
            
            month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            month_name = month_names[month] if 1 <= month <= 12 else "Invalid"
            
            print(f"  {location} - {year}-{month:02d} ({month_name}): {count} transactions, Rp {sales:,.0f}")
        
        # Test 5: Compare with existing month/year fields
        print("\n" + "="*60)
        print("TEST 5: Compare extracted vs existing month/year fields")
        print("="*60)
        
        pipeline5 = [
            {
                "$addFields": {
                    "extracted_month": {
                        "$toInt": {
                            "$substr": ["$Sales Date", 3, 2]
                        }
                    },
                    "extracted_year": {
                        "$toInt": {
                            "$substr": ["$Sales Date", 6, 4]
                        }
                    }
                }
            },
            {
                "$project": {
                    "Sales Date": 1,
                    "existing_month": "$month",
                    "existing_year": "$year",
                    "extracted_month": 1,
                    "extracted_year": 1,
                    "month_match": {"$eq": ["$month", "$extracted_month"]},
                    "year_match": {"$eq": ["$year", "$extracted_year"]}
                }
            },
            {"$limit": 10}
        ]
        
        print(f"Pipeline: {json.dumps(pipeline5, indent=2)}")
        
        results5 = list(collection.aggregate(pipeline5))
        print(f"\nResults: Comparison of first 10 records")
        for result in results5:
            date = result['Sales Date']
            existing_m = result.get('existing_month', 'None')
            existing_y = result.get('existing_year', 'None')
            extracted_m = result['extracted_month']
            extracted_y = result['extracted_year']
            m_match = result['month_match']
            y_match = result['year_match']
            
            print(f"  {date}: existing({existing_y}-{existing_m}) vs extracted({extracted_y}-{extracted_m}) - Match: {m_match and y_match}")
        
        mongo_conn.disconnect()
        print("\n🎉 Sales Date aggregation tests complete!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_sales_date_aggregations()