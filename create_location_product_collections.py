#!/usr/bin/env python3
"""
Create Location and Product Collections by Time Periods
For Sales Trend, Location Performance, and Product Category charts with date range filtering
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

def create_location_by_week_collection():
    """Create sales_by_location_week collection from transaction_sales"""
    logger.info("🚀 Creating sales_by_location_week collection...")
    
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    
    if not client:
        logger.error("❌ Failed to connect to MongoDB")
        return False
    
    db = mongo_conn.get_database()
    
    # Drop existing collection if it exists
    if 'sales_by_location_week' in db.list_collection_names():
        db.drop_collection('sales_by_location_week')
        logger.info("🗑️ Dropped existing sales_by_location_week collection")
    
    # Create aggregation pipeline for weekly location data
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
                }
            }
        },
        {
            "$match": {
                "parsed_date": {"$ne": None},
                "Location Name": {"$ne": None}
            }
        },
        {
            "$addFields": {
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
            "$group": {
                "_id": {
                    "iso_week": {"$isoWeek": "$parsed_date"},
                    "iso_week_year": {"$isoWeekYear": "$parsed_date"},
                    "location_name": "$Location Name"
                },
                "total_sales": {"$sum": "$total_numeric"},
                "total_transactions": {"$sum": 1},
                "avg_transaction": {"$avg": "$total_numeric"},
                "start_date": {"$min": "$parsed_date"},
                "end_date": {"$max": "$parsed_date"}
            }
        },
        {
            "$addFields": {
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
                }
            }
        },
        {
            "$project": {
                "year": "$_id.iso_week_year",
                "iso_week": "$_id.iso_week",
                "week_label": 1,
                "location_name": "$_id.location_name",
                "total_sales": {"$round": ["$total_sales", 2]},
                "total_transactions": 1,
                "avg_transaction": {"$round": ["$avg_transaction", 2]},
                "start_date": 1,
                "end_date": 1,
                "_id": 0
            }
        },
        {
            "$sort": {
                "year": 1,
                "iso_week": 1,
                "location_name": 1
            }
        },
        {
            "$out": "sales_by_location_week"
        }
    ]
    
    try:
        # Execute aggregation
        collection = db['transaction_sales']
        result = list(collection.aggregate(pipeline, allowDiskUse=True))
        
        # Check results
        location_week_collection = db['sales_by_location_week']
        count = location_week_collection.count_documents({})
        
        logger.info(f"✅ Created sales_by_location_week collection with {count} documents")
        
        # Show sample document
        sample = location_week_collection.find_one()
        if sample:
            logger.info(f"📄 Sample document: {json.dumps(sample, indent=2, default=str)}")
        
        mongo_conn.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating sales_by_location_week collection: {e}")
        mongo_conn.disconnect()
        return False

def create_location_by_day_collection():
    """Create sales_by_location_day collection from transaction_sales"""
    logger.info("🚀 Creating sales_by_location_day collection...")
    
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    
    if not client:
        logger.error("❌ Failed to connect to MongoDB")
        return False
    
    db = mongo_conn.get_database()
    
    # Drop existing collection if it exists
    if 'sales_by_location_day' in db.list_collection_names():
        db.drop_collection('sales_by_location_day')
        logger.info("🗑️ Dropped existing sales_by_location_day collection")
    
    # Create aggregation pipeline for daily location data
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
                }
            }
        },
        {
            "$match": {
                "parsed_date": {"$ne": None},
                "Location Name": {"$ne": None}
            }
        },
        {
            "$addFields": {
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
            "$group": {
                "_id": {
                    "year": {"$year": "$parsed_date"},
                    "month": {"$month": "$parsed_date"},
                    "day": {"$dayOfMonth": "$parsed_date"},
                    "location_name": "$Location Name"
                },
                "total_sales": {"$sum": "$total_numeric"},
                "total_transactions": {"$sum": 1},
                "avg_transaction": {"$avg": "$total_numeric"},
                "date": {"$first": {"$dateFromParts": {
                    "year": {"$year": "$parsed_date"},
                    "month": {"$month": "$parsed_date"},
                    "day": {"$dayOfMonth": "$parsed_date"}
                }}}
            }
        },
        {
            "$addFields": {
                "display_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$date"
                    }
                }
            }
        },
        {
            "$project": {
                "year": "$_id.year",
                "month": "$_id.month", 
                "day": "$_id.day",
                "date": 1,
                "display_date": 1,
                "location_name": "$_id.location_name",
                "total_sales": {"$round": ["$total_sales", 2]},
                "total_transactions": 1,
                "avg_transaction": {"$round": ["$avg_transaction", 2]},
                "_id": 0
            }
        },
        {
            "$sort": {
                "date": 1,
                "location_name": 1
            }
        },
        {
            "$out": "sales_by_location_day"
        }
    ]
    
    try:
        # Execute aggregation
        collection = db['transaction_sales']
        result = list(collection.aggregate(pipeline, allowDiskUse=True))
        
        # Check results
        location_day_collection = db['sales_by_location_day']
        count = location_day_collection.count_documents({})
        
        logger.info(f"✅ Created sales_by_location_day collection with {count} documents")
        
        # Show sample document
        sample = location_day_collection.find_one()
        if sample:
            logger.info(f"📄 Sample document: {json.dumps(sample, indent=2, default=str)}")
        
        mongo_conn.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating sales_by_location_day collection: {e}")
        mongo_conn.disconnect()
        return False

def create_product_by_month_collection():
    """Create sales_by_product_month collection from transaction_sales"""
    logger.info("🚀 Creating sales_by_product_month collection...")
    
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    
    if not client:
        logger.error("❌ Failed to connect to MongoDB")
        return False
    
    db = mongo_conn.get_database()
    
    # Drop existing collection if it exists
    if 'sales_by_product_month' in db.list_collection_names():
        db.drop_collection('sales_by_product_month')
        logger.info("🗑️ Dropped existing sales_by_product_month collection")
    
    # Create aggregation pipeline for monthly product data
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
                }
            }
        },
        {
            "$match": {
                "parsed_date": {"$ne": None},
                "Product Name": {"$ne": None}
            }
        },
        {
            "$addFields": {
                "total_numeric": {
                    "$toDouble": {
                        "$replaceAll": {
                            "input": {"$toString": "$Total"},
                            "find": ",",
                            "replacement": ""
                        }
                    }
                },
                "quantity_numeric": {
                    "$toDouble": {
                        "$replaceAll": {
                            "input": {"$toString": "$Quantity"},
                            "find": ",",
                            "replacement": ""
                        }
                    }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$parsed_date"},
                    "month": {"$month": "$parsed_date"},
                    "product_name": "$Product Name"
                },
                "total_revenue": {"$sum": "$total_numeric"},
                "total_quantity_sold": {"$sum": "$quantity_numeric"},
                "total_transactions": {"$sum": 1},
                "avg_transaction": {"$avg": "$total_numeric"}
            }
        },
        {
            "$addFields": {
                "month_name": {
                    "$arrayElemAt": [
                        ["", "January", "February", "March", "April", "May", "June",
                         "July", "August", "September", "October", "November", "December"],
                        "$_id.month"
                    ]
                }
            }
        },
        {
            "$project": {
                "year": "$_id.year",
                "month": "$_id.month",
                "month_name": 1,
                "product_name": "$_id.product_name",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "total_quantity_sold": 1,
                "total_transactions": 1,
                "avg_transaction": {"$round": ["$avg_transaction", 2]},
                "_id": 0
            }
        },
        {
            "$sort": {
                "year": 1,
                "month": 1,
                "product_name": 1
            }
        },
        {
            "$out": "sales_by_product_month"
        }
    ]
    
    try:
        # Execute aggregation
        collection = db['transaction_sales']
        result = list(collection.aggregate(pipeline, allowDiskUse=True))
        
        # Check results
        product_month_collection = db['sales_by_product_month']
        count = product_month_collection.count_documents({})
        
        logger.info(f"✅ Created sales_by_product_month collection with {count} documents")
        
        # Show sample document
        sample = product_month_collection.find_one()
        if sample:
            logger.info(f"📄 Sample document: {json.dumps(sample, indent=2, default=str)}")
        
        mongo_conn.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating sales_by_product_month collection: {e}")
        mongo_conn.disconnect()
        return False

def create_product_by_week_collection():
    """Create sales_by_product_week collection from transaction_sales"""
    logger.info("🚀 Creating sales_by_product_week collection...")
    
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    
    if not client:
        logger.error("❌ Failed to connect to MongoDB")
        return False
    
    db = mongo_conn.get_database()
    
    # Drop existing collection if it exists
    if 'sales_by_product_week' in db.list_collection_names():
        db.drop_collection('sales_by_product_week')
        logger.info("🗑️ Dropped existing sales_by_product_week collection")
    
    # Create aggregation pipeline for weekly product data
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
                }
            }
        },
        {
            "$match": {
                "parsed_date": {"$ne": None},
                "Product Name": {"$ne": None}
            }
        },
        {
            "$addFields": {
                "total_numeric": {
                    "$toDouble": {
                        "$replaceAll": {
                            "input": {"$toString": "$Total"},
                            "find": ",",
                            "replacement": ""
                        }
                    }
                },
                "quantity_numeric": {
                    "$toDouble": {
                        "$replaceAll": {
                            "input": {"$toString": "$Quantity"},
                            "find": ",",
                            "replacement": ""
                        }
                    }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "iso_week": {"$isoWeek": "$parsed_date"},
                    "iso_week_year": {"$isoWeekYear": "$parsed_date"},
                    "product_name": "$Product Name"
                },
                "total_revenue": {"$sum": "$total_numeric"},
                "total_quantity_sold": {"$sum": "$quantity_numeric"},
                "total_transactions": {"$sum": 1},
                "avg_transaction": {"$avg": "$total_numeric"},
                "start_date": {"$min": "$parsed_date"},
                "end_date": {"$max": "$parsed_date"}
            }
        },
        {
            "$addFields": {
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
                }
            }
        },
        {
            "$project": {
                "year": "$_id.iso_week_year",
                "iso_week": "$_id.iso_week",
                "week_label": 1,
                "product_name": "$_id.product_name",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "total_quantity_sold": 1,
                "total_transactions": 1,
                "avg_transaction": {"$round": ["$avg_transaction", 2]},
                "start_date": 1,
                "end_date": 1,
                "_id": 0
            }
        },
        {
            "$sort": {
                "year": 1,
                "iso_week": 1,
                "product_name": 1
            }
        },
        {
            "$out": "sales_by_product_week"
        }
    ]
    
    try:
        # Execute aggregation
        collection = db['transaction_sales']
        result = list(collection.aggregate(pipeline, allowDiskUse=True))
        
        # Check results
        product_week_collection = db['sales_by_product_week']
        count = product_week_collection.count_documents({})
        
        logger.info(f"✅ Created sales_by_product_week collection with {count} documents")
        
        # Show sample document
        sample = product_week_collection.find_one()
        if sample:
            logger.info(f"📄 Sample document: {json.dumps(sample, indent=2, default=str)}")
        
        mongo_conn.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating sales_by_product_week collection: {e}")
        mongo_conn.disconnect()
        return False

def create_product_by_day_collection():
    """Create sales_by_product_day collection from transaction_sales"""
    logger.info("🚀 Creating sales_by_product_day collection...")
    
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    
    if not client:
        logger.error("❌ Failed to connect to MongoDB")
        return False
    
    db = mongo_conn.get_database()
    
    # Drop existing collection if it exists
    if 'sales_by_product_day' in db.list_collection_names():
        db.drop_collection('sales_by_product_day')
        logger.info("🗑️ Dropped existing sales_by_product_day collection")
    
    # Create aggregation pipeline for daily product data
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
                }
            }
        },
        {
            "$match": {
                "parsed_date": {"$ne": None},
                "Product Name": {"$ne": None}
            }
        },
        {
            "$addFields": {
                "total_numeric": {
                    "$toDouble": {
                        "$replaceAll": {
                            "input": {"$toString": "$Total"},
                            "find": ",",
                            "replacement": ""
                        }
                    }
                },
                "quantity_numeric": {
                    "$toDouble": {
                        "$replaceAll": {
                            "input": {"$toString": "$Quantity"},
                            "find": ",",
                            "replacement": ""
                        }
                    }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$parsed_date"},
                    "month": {"$month": "$parsed_date"},
                    "day": {"$dayOfMonth": "$parsed_date"},
                    "product_name": "$Product Name"
                },
                "total_revenue": {"$sum": "$total_numeric"},
                "total_quantity_sold": {"$sum": "$quantity_numeric"},
                "total_transactions": {"$sum": 1},
                "avg_transaction": {"$avg": "$total_numeric"},
                "date": {"$first": {"$dateFromParts": {
                    "year": {"$year": "$parsed_date"},
                    "month": {"$month": "$parsed_date"},
                    "day": {"$dayOfMonth": "$parsed_date"}
                }}}
            }
        },
        {
            "$addFields": {
                "display_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$date"
                    }
                }
            }
        },
        {
            "$project": {
                "year": "$_id.year",
                "month": "$_id.month", 
                "day": "$_id.day",
                "date": 1,
                "display_date": 1,
                "product_name": "$_id.product_name",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "total_quantity_sold": 1,
                "total_transactions": 1,
                "avg_transaction": {"$round": ["$avg_transaction", 2]},
                "_id": 0
            }
        },
        {
            "$sort": {
                "date": 1,
                "product_name": 1
            }
        },
        {
            "$out": "sales_by_product_day"
        }
    ]
    
    try:
        # Execute aggregation
        collection = db['transaction_sales']
        result = list(collection.aggregate(pipeline, allowDiskUse=True))
        
        # Check results
        product_day_collection = db['sales_by_product_day']
        count = product_day_collection.count_documents({})
        
        logger.info(f"✅ Created sales_by_product_day collection with {count} documents")
        
        # Show sample document
        sample = product_day_collection.find_one()
        if sample:
            logger.info(f"📄 Sample document: {json.dumps(sample, indent=2, default=str)}")
        
        mongo_conn.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating sales_by_product_day collection: {e}")
        mongo_conn.disconnect()
        return False

def create_all_location_product_collections():
    """Create all location and product collections"""
    logger.info("🚀 Creating all location and product collections...")
    
    success_count = 0
    
    # Create location collections
    if create_location_by_week_collection():
        success_count += 1
    
    if create_location_by_day_collection():
        success_count += 1
    
    # Create product collections
    if create_product_by_month_collection():
        success_count += 1
    
    if create_product_by_week_collection():
        success_count += 1
    
    if create_product_by_day_collection():
        success_count += 1
    
    logger.info(f"✅ Successfully created {success_count}/5 location and product collections")
    return success_count == 5

if __name__ == "__main__":
    print("🚀 Creating Location and Product Collections by Time Periods")
    print("=" * 70)
    
    success = create_all_location_product_collections()
    
    if success:
        print("✅ All location and product collections created successfully!")
    else:
        print("❌ Some collections failed to create. Check logs for details.")