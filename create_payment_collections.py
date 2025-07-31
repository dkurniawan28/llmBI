#!/usr/bin/env python3
"""
Create Payment Method Collections by Time Periods
For Payment Method Trends with date range filtering
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

def create_payment_by_week_collection():
    """Create payment_by_week collection from transaction_sales"""
    logger.info("🚀 Creating payment_by_week collection...")
    
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    
    if not client:
        logger.error("❌ Failed to connect to MongoDB")
        return False
    
    db = mongo_conn.get_database()
    
    # Drop existing collection if it exists
    if 'payment_by_week' in db.list_collection_names():
        db.drop_collection('payment_by_week')
        logger.info("🗑️ Dropped existing payment_by_week collection")
    
    # Create aggregation pipeline for weekly payment data
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
                "Payment Method": {"$ne": None}
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
                    "payment_method": "$Payment Method"
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
                "payment_method": "$_id.payment_method",
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
                "payment_method": 1
            }
        },
        {
            "$out": "payment_by_week"
        }
    ]
    
    try:
        # Execute aggregation
        collection = db['transaction_sales']
        result = list(collection.aggregate(pipeline, allowDiskUse=True))
        
        # Check results
        payment_week_collection = db['payment_by_week']
        count = payment_week_collection.count_documents({})
        
        logger.info(f"✅ Created payment_by_week collection with {count} documents")
        
        # Show sample document
        sample = payment_week_collection.find_one()
        if sample:
            logger.info(f"📄 Sample document: {json.dumps(sample, indent=2, default=str)}")
        
        mongo_conn.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating payment_by_week collection: {e}")
        mongo_conn.disconnect()
        return False

def create_payment_by_day_collection():
    """Create payment_by_day collection from transaction_sales"""
    logger.info("🚀 Creating payment_by_day collection...")
    
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    
    if not client:
        logger.error("❌ Failed to connect to MongoDB")
        return False
    
    db = mongo_conn.get_database()
    
    # Drop existing collection if it exists
    if 'payment_by_day' in db.list_collection_names():
        db.drop_collection('payment_by_day')
        logger.info("🗑️ Dropped existing payment_by_day collection")
    
    # Create aggregation pipeline for daily payment data
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
                "Payment Method": {"$ne": None}
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
                    "payment_method": "$Payment Method"
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
                "payment_method": "$_id.payment_method",
                "total_sales": {"$round": ["$total_sales", 2]},
                "total_transactions": 1,
                "avg_transaction": {"$round": ["$avg_transaction", 2]},
                "_id": 0
            }
        },
        {
            "$sort": {
                "date": 1,
                "payment_method": 1
            }
        },
        {
            "$out": "payment_by_day"
        }
    ]
    
    try:
        # Execute aggregation
        collection = db['transaction_sales']
        result = list(collection.aggregate(pipeline, allowDiskUse=True))
        
        # Check results
        payment_day_collection = db['payment_by_day']
        count = payment_day_collection.count_documents({})
        
        logger.info(f"✅ Created payment_by_day collection with {count} documents")
        
        # Show sample document
        sample = payment_day_collection.find_one()
        if sample:
            logger.info(f"📄 Sample document: {json.dumps(sample, indent=2, default=str)}")
        
        mongo_conn.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating payment_by_day collection: {e}")
        mongo_conn.disconnect()
        return False

def create_payment_by_month_collection():
    """Create payment_by_month collection from existing sales_by_payment_method"""
    logger.info("🚀 Creating payment_by_month collection...")
    
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    
    if not client:
        logger.error("❌ Failed to connect to MongoDB")
        return False
    
    db = mongo_conn.get_database()
    
    # Drop existing collection if it exists
    if 'payment_by_month' in db.list_collection_names():
        db.drop_collection('payment_by_month')
        logger.info("🗑️ Dropped existing payment_by_month collection")
    
    # Create aggregation pipeline for monthly payment data
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
                "Payment Method": {"$ne": None}
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
                    "payment_method": "$Payment Method"
                },
                "total_sales": {"$sum": "$total_numeric"},
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
                "payment_method": "$_id.payment_method",
                "total_sales": {"$round": ["$total_sales", 2]},
                "total_transactions": 1,
                "avg_transaction": {"$round": ["$avg_transaction", 2]},
                "_id": 0
            }
        },
        {
            "$sort": {
                "year": 1,
                "month": 1,
                "payment_method": 1
            }
        },
        {
            "$out": "payment_by_month"
        }
    ]
    
    try:
        # Execute aggregation
        collection = db['transaction_sales']
        result = list(collection.aggregate(pipeline, allowDiskUse=True))
        
        # Check results
        payment_month_collection = db['payment_by_month']
        count = payment_month_collection.count_documents({})
        
        logger.info(f"✅ Created payment_by_month collection with {count} documents")
        
        # Show sample document
        sample = payment_month_collection.find_one()
        if sample:
            logger.info(f"📄 Sample document: {json.dumps(sample, indent=2, default=str)}")
        
        mongo_conn.disconnect()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating payment_by_month collection: {e}")
        mongo_conn.disconnect()
        return False

def create_all_payment_collections():
    """Create all payment method collections"""
    logger.info("🚀 Creating all payment method collections...")
    
    success_count = 0
    
    # Create weekly collection
    if create_payment_by_week_collection():
        success_count += 1
    
    # Create daily collection
    if create_payment_by_day_collection():
        success_count += 1
    
    # Create monthly collection
    if create_payment_by_month_collection():
        success_count += 1
    
    logger.info(f"✅ Successfully created {success_count}/3 payment method collections")
    return success_count == 3

if __name__ == "__main__":
    print("🚀 Creating Payment Method Collections by Time Periods")
    print("=" * 60)
    
    success = create_all_payment_collections()
    
    if success:
        print("✅ All payment method collections created successfully!")
    else:
        print("❌ Some collections failed to create. Check logs for details.")