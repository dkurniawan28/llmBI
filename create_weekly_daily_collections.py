#!/usr/bin/env python3
"""
Create Weekly and Daily Sales Collections
For better performance in candlestick charts
"""

# Load environment variables first
import load_env

from mongodb_connection import MongoDBSSHConnection
import json
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_sales_by_week_collection():
    """Create sales_by_week collection from transaction_sales"""
    logger.info("🚀 Creating sales_by_week collection...")
    
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    
    if not client:
        logger.error("❌ Failed to connect to MongoDB")
        return False
    
    db = mongo_conn.get_database()
    
    # Drop existing collection if it exists
    if 'sales_by_week' in db.list_collection_names():
        db.drop_collection('sales_by_week')
        logger.info("🗑️ Dropped existing sales_by_week collection")
    
    # Create aggregation pipeline for weekly data
    pipeline = [
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
                    "$switch": {
                        "branches": [
                            {
                                "case": {"$eq": [{"$type": "$Total"}, "string"]},
                                "then": {
                                    "$toDouble": {
                                        "$replaceAll": {
                                            "input": "$Total",
                                            "find": ",",
                                            "replacement": "."
                                        }
                                    }
                                }
                            },
                            {
                                "case": {"$eq": [{"$type": "$Total"}, "double"]},
                                "then": "$Total"
                            },
                            {
                                "case": {"$eq": [{"$type": "$Total"}, "int"]},
                                "then": {"$toDouble": "$Total"}
                            }
                        ],
                        "default": 0
                    }
                }
            }
        },
        {
            "$match": {
                "parsed_date": {"$ne": None},
                "total_numeric": {"$gt": 0}
            }
        },
        {
            "$addFields": {
                "year": {"$year": "$parsed_date"},
                "week": {"$week": "$parsed_date"},
                "iso_week": {"$isoWeek": "$parsed_date"},
                "iso_week_year": {"$isoWeekYear": "$parsed_date"}
            }
        },
        {
            "$group": {
                "_id": {
                    "iso_week": "$iso_week",
                    "iso_week_year": "$iso_week_year"
                },
                "total_sales": {"$sum": "$total_numeric"},
                "total_transactions": {"$sum": 1},
                "avg_transaction": {"$avg": "$total_numeric"},
                "min_transaction": {"$min": "$total_numeric"},
                "max_transaction": {"$max": "$total_numeric"},
                "start_date": {"$min": "$parsed_date"},
                "end_date": {"$max": "$parsed_date"},
                "year": {"$first": "$year"},
                "week": {"$first": "$week"}
            }
        },
        {
            "$project": {
                "year": 1,
                "week": 1,
                "iso_week": "$_id.iso_week",
                "iso_week_year": "$_id.iso_week_year",
                "total_sales": 1,
                "total_transactions": 1,
                "avg_transaction": 1,
                "min_transaction": 1,
                "max_transaction": 1,
                "start_date": 1,
                "end_date": 1,
                "week_label": {
                    "$concat": [
                        {"$toString": "$_id.iso_week_year"},
                        "-W",
                        {
                            "$cond": {
                                "if": {"$lt": ["$_id.iso_week", 10]},
                                "then": {"$concat": ["0", {"$toString": "$_id.iso_week"}]},
                                "else": {"$toString": "$_id.iso_week"}
                            }
                        }
                    ]
                },
                "_id": 0
            }
        },
        {
            "$sort": {"iso_week_year": 1, "iso_week": 1}
        },
        {
            "$out": "sales_by_week"
        }
    ]
    
    try:
        # Execute aggregation
        result = list(db.transaction_sales.aggregate(pipeline))
        
        # Check if collection was created
        doc_count = db.sales_by_week.count_documents({})
        logger.info(f"✅ Created sales_by_week collection with {doc_count} documents")
        
        # Create index for better performance
        db.sales_by_week.create_index([("year", 1), ("iso_week", 1)])
        db.sales_by_week.create_index([("iso_week_year", 1), ("iso_week", 1)])
        
        mongo_conn.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating weekly collection: {e}")
        mongo_conn.disconnect()
        return False

def create_sales_by_day_collection():
    """Create sales_by_day collection from transaction_sales"""
    logger.info("🗓️ Creating sales_by_day collection...")
    
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    
    if not client:
        logger.error("❌ Failed to connect to MongoDB")
        return False
    
    db = mongo_conn.get_database()
    
    # Drop existing collection if it exists
    if 'sales_by_day' in db.list_collection_names():
        db.drop_collection('sales_by_day')
        logger.info("🗑️ Dropped existing sales_by_day collection")
    
    # Create aggregation pipeline for daily data
    pipeline = [
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
                    "$switch": {
                        "branches": [
                            {
                                "case": {"$eq": [{"$type": "$Total"}, "string"]},
                                "then": {
                                    "$toDouble": {
                                        "$replaceAll": {
                                            "input": "$Total",
                                            "find": ",",
                                            "replacement": "."
                                        }
                                    }
                                }
                            },
                            {
                                "case": {"$eq": [{"$type": "$Total"}, "double"]},
                                "then": "$Total"
                            },
                            {
                                "case": {"$eq": [{"$type": "$Total"}, "int"]},
                                "then": {"$toDouble": "$Total"}
                            }
                        ],
                        "default": 0
                    }
                }
            }
        },
        {
            "$match": {
                "parsed_date": {"$ne": None},
                "total_numeric": {"$gt": 0}
            }
        },
        {
            "$addFields": {
                "year": {"$year": "$parsed_date"},
                "month": {"$month": "$parsed_date"},
                "day": {"$dayOfMonth": "$parsed_date"},
                "day_of_week": {"$dayOfWeek": "$parsed_date"},
                "day_of_year": {"$dayOfYear": "$parsed_date"},
                "date_only": {
                    "$dateFromParts": {
                        "year": {"$year": "$parsed_date"},
                        "month": {"$month": "$parsed_date"},
                        "day": {"$dayOfMonth": "$parsed_date"}
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$date_only",
                "total_sales": {"$sum": "$total_numeric"},
                "total_transactions": {"$sum": 1},
                "avg_transaction": {"$avg": "$total_numeric"},
                "min_transaction": {"$min": "$total_numeric"},
                "max_transaction": {"$max": "$total_numeric"},
                "first_transaction_time": {"$min": "$parsed_date"},
                "last_transaction_time": {"$max": "$parsed_date"}
            }
        },
        {
            "$project": {
                "date": "$_id",
                "year": {"$year": "$_id"},
                "month": {"$month": "$_id"},
                "day": {"$dayOfMonth": "$_id"},
                "day_of_week": {"$dayOfWeek": "$_id"},
                "day_of_year": {"$dayOfYear": "$_id"},
                "total_sales": 1,
                "total_transactions": 1,
                "avg_transaction": 1,
                "min_transaction": 1,
                "max_transaction": 1,
                "first_transaction_time": 1,
                "last_transaction_time": 1,
                "date_string": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$_id"
                    }
                },
                "display_date": {
                    "$dateToString": {
                        "format": "%m/%d",
                        "date": "$_id"
                    }
                },
                "_id": 0
            }
        },
        {
            "$sort": {"date": 1}
        },
        {
            "$out": "sales_by_day"
        }
    ]
    
    try:
        # Execute aggregation
        result = list(db.transaction_sales.aggregate(pipeline))
        
        # Check if collection was created
        doc_count = db.sales_by_day.count_documents({})
        logger.info(f"✅ Created sales_by_day collection with {doc_count} documents")
        
        # Create indexes for better performance
        db.sales_by_day.create_index([("year", 1), ("month", 1), ("day", 1)])
        db.sales_by_day.create_index([("date", 1)])
        
        mongo_conn.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating daily collection: {e}")
        mongo_conn.disconnect()
        return False

def create_collection_schemas():
    """Create JSON schemas for the new collections"""
    
    # Weekly collection schema
    weekly_schema = {
        "collection_name": "sales_by_week",
        "description": "Weekly aggregated sales data for candlestick charts",
        "sample_document": {
            "year": 2025,
            "week": 17,
            "iso_week": 17,
            "iso_week_year": 2025,
            "total_sales": 123456789.0,
            "total_transactions": 5432,
            "avg_transaction": 22725.0,
            "min_transaction": 15000.0,
            "max_transaction": 45000.0,
            "start_date": "2025-04-21T00:00:00Z",
            "end_date": "2025-04-27T23:59:59Z",
            "week_label": "2025-W17"
        },
        "fields": {
            "year": "int - Calendar year",
            "week": "int - Week number (1-53)",
            "iso_week": "int - ISO week number",
            "iso_week_year": "int - ISO week year",
            "total_sales": "double - Sum of all sales for the week",
            "total_transactions": "int - Number of transactions",
            "avg_transaction": "double - Average transaction value",
            "min_transaction": "double - Minimum transaction value",
            "max_transaction": "double - Maximum transaction value",
            "start_date": "date - First day of the week",
            "end_date": "date - Last day of the week",
            "week_label": "string - Human readable week label (YYYY-WNN)"
        }
    }
    
    # Daily collection schema
    daily_schema = {
        "collection_name": "sales_by_day",
        "description": "Daily aggregated sales data for candlestick charts",
        "sample_document": {
            "date": "2025-04-15T00:00:00Z",
            "year": 2025,
            "month": 4,
            "day": 15,
            "day_of_week": 3,
            "day_of_year": 105,
            "total_sales": 12345678.0,
            "total_transactions": 543,
            "avg_transaction": 22725.0,
            "min_transaction": 15000.0,
            "max_transaction": 45000.0,
            "first_transaction_time": "2025-04-15T08:30:00Z",
            "last_transaction_time": "2025-04-15T21:45:00Z",
            "date_string": "2025-04-15",
            "display_date": "04/15"
        },
        "fields": {
            "date": "date - The specific date",
            "year": "int - Calendar year",
            "month": "int - Month (1-12)",
            "day": "int - Day of month (1-31)",
            "day_of_week": "int - Day of week (1=Sunday, 7=Saturday)",
            "day_of_year": "int - Day of year (1-366)",
            "total_sales": "double - Sum of all sales for the day",
            "total_transactions": "int - Number of transactions",
            "avg_transaction": "double - Average transaction value",
            "min_transaction": "double - Minimum transaction value",
            "max_transaction": "double - Maximum transaction value",
            "first_transaction_time": "date - Time of first transaction",
            "last_transaction_time": "date - Time of last transaction",
            "date_string": "string - Date in YYYY-MM-DD format",
            "display_date": "string - Date in MM/DD format for display"
        }
    }
    
    # Save schemas
    with open('support/sales_by_week.json', 'w') as f:
        json.dump(weekly_schema, f, indent=2, default=str)
    
    with open('support/sales_by_day.json', 'w') as f:
        json.dump(daily_schema, f, indent=2, default=str)
    
    logger.info("✅ Created JSON schemas for new collections")

def main():
    """Main function to create weekly and daily collections"""
    logger.info("🚀 Starting creation of weekly and daily sales collections...")
    
    # Create weekly collection
    weekly_success = create_sales_by_week_collection()
    
    # Create daily collection
    daily_success = create_sales_by_day_collection()
    
    # Create schemas
    create_collection_schemas()
    
    if weekly_success and daily_success:
        logger.info("✅ Successfully created both weekly and daily collections!")
        logger.info("📊 Collections available:")
        logger.info("   - sales_by_week: For weekly candlestick data")
        logger.info("   - sales_by_day: For daily candlestick data")
        logger.info("   - JSON schemas saved in support/ directory")
    else:
        logger.error("❌ Failed to create one or more collections")
        if not weekly_success:
            logger.error("   - sales_by_week creation failed")
        if not daily_success:
            logger.error("   - sales_by_day creation failed")

if __name__ == "__main__":
    main()