#!/usr/bin/env python3
"""
Create Master Location Collection
Extract unique locations from transaction_sales for dynamic dropdowns
"""

# Load environment variables first
import load_env

from mongodb_connection import MongoDBSSHConnection
import json
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_master_location_collection():
    """Create master_locations collection from transaction_sales"""
    logger.info("🚀 Creating master_locations collection...")
    
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    
    if not client:
        logger.error("❌ Failed to connect to MongoDB")
        return False
    
    db = mongo_conn.get_database()
    
    # Drop existing collection if it exists
    if 'master_locations' in db.list_collection_names():
        db.drop_collection('master_locations')
        logger.info("🗑️ Dropped existing master_locations collection")
    
    # Create aggregation pipeline to get unique locations with stats
    pipeline = [
        {
            "$match": {
                "Location Name": {"$ne": None, "$ne": ""}
            }
        },
        {
            "$addFields": {
                "parsed_date": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {"$eq": [{"$type": "$Sales Date"}, "string"]},
                                "then": {
                                    "$dateFromString": {
                                        "dateString": "$Sales Date",
                                        "format": "%d/%m/%Y",
                                        "onError": None
                                    }
                                }
                            },
                            {
                                "case": {"$eq": [{"$type": "$Sales Date"}, "date"]},
                                "then": "$Sales Date"
                            }
                        ],
                        "default": None
                    }
                },
                "total_numeric": {
                    "$toDouble": {
                        "$replaceAll": {
                            "input": {"$toString": "$Total"},
                            "find": ",",
                            "replacement": ""
                        }
                    }
                }
            }
        },
        {
            "$match": {
                "parsed_date": {"$ne": None}
            }
        },
        {
            "$group": {
                "_id": "$Location Name",
                "total_sales": {"$sum": "$total_numeric"},
                "total_transactions": {"$sum": 1},
                "avg_transaction": {"$avg": "$total_numeric"},
                "first_transaction": {"$min": "$parsed_date"},
                "last_transaction": {"$max": "$parsed_date"},
                "unique_products": {"$addToSet": "$Product Name"}
            }
        },
        {
            "$addFields": {
                "product_count": {"$size": "$unique_products"},
                "days_active": {
                    "$divide": [
                        {"$subtract": ["$last_transaction", "$first_transaction"]},
                        1000 * 60 * 60 * 24  # Convert milliseconds to days
                    ]
                }
            }
        },
        {
            "$project": {
                "location_name": "$_id",
                "total_sales": {"$round": ["$total_sales", 2]},
                "total_transactions": 1,
                "avg_transaction": {"$round": ["$avg_transaction", 2]},
                "first_transaction": 1,
                "last_transaction": 1,
                "product_count": 1,
                "days_active": {"$round": ["$days_active", 0]},
                "is_active": {
                    "$gte": [
                        "$last_transaction",
                        {"$dateSubtract": {
                            "startDate": "$$NOW",
                            "unit": "day",
                            "amount": 365  # Changed to 1 year for older data
                        }}
                    ]
                },
                "_id": 0
            }
        },
        {
            "$sort": {
                "total_sales": -1  # Sort by highest sales first
            }
        },
        {
            "$out": "master_locations"
        }
    ]
    
    try:
        # Execute aggregation
        collection = db['transaction_sales']
        result = list(collection.aggregate(pipeline, allowDiskUse=True))
        
        # Check results
        master_locations_collection = db['master_locations']
        count = master_locations_collection.count_documents({})
        
        logger.info(f"✅ Created master_locations collection with {count} locations")
        
        # Show sample documents
        active_locations = list(master_locations_collection.find({"is_active": True}).limit(5))
        logger.info(f"📄 Sample active locations: {len(active_locations)}")
        for loc in active_locations:
            logger.info(f"  - {loc['location_name']}: Rp {loc['total_sales']:,.0f} ({loc['total_transactions']} transactions)")
        
        # Show inactive locations count
        inactive_count = master_locations_collection.count_documents({"is_active": False})
        logger.info(f"📊 Active locations: {count - inactive_count}, Inactive: {inactive_count}")
        
        mongo_conn.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating master_locations collection: {e}")
        mongo_conn.disconnect()
        return False

def get_location_options():
    """Get location options for dropdown (for testing)"""
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    
    if not client:
        return []
    
    db = mongo_conn.get_database()
    
    try:
        # Get all locations sorted by sales (remove active filter for older data)
        locations = list(db['master_locations'].find(
            {},  # Get all locations regardless of active status
            {"location_name": 1, "total_sales": 1, "_id": 0}
        ).sort("total_sales", -1).limit(50))  # Limit to top 50 by sales
        
        location_names = [loc['location_name'] for loc in locations]
        logger.info(f"📋 Found {len(location_names)} active locations for dropdown")
        
        mongo_conn.disconnect()
        return location_names
        
    except Exception as e:
        logger.error(f"❌ Error getting location options: {e}")
        mongo_conn.disconnect()
        return []

if __name__ == "__main__":
    print("🚀 Creating Master Location Collection")
    print("=" * 50)
    
    success = create_master_location_collection()
    
    if success:
        print("✅ Master location collection created successfully!")
        
        # Test getting location options
        print("\n🧪 Testing location options...")
        options = get_location_options()
        print(f"📋 Available locations: {len(options)}")
        if options:
            print("Top 10 locations by sales:")
            for i, loc in enumerate(options[:10], 1):
                print(f"  {i}. {loc}")
    else:
        print("❌ Failed to create master location collection.")