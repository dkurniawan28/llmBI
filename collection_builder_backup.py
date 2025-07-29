#!/usr/bin/env python3

from mongodb_connection import MongoDBSSHConnection
import json
import os
from datetime import datetime
import requests

class OptimizedCollectionBuilder:
    def __init__(self):
        self.mongo_conn = MongoDBSSHConnection()
        self.db = None
        self.OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY') or "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"
        
        # Define collection builders
        self.collection_builders = {
            'sales_by_location': self.build_sales_by_location,
            'sales_by_month': self.build_sales_by_month,
            'sales_by_location_month': self.build_sales_by_location_month,
            'sales_by_product': self.build_sales_by_product,
            'sales_by_payment_method': self.build_sales_by_payment_method,
            'sales_summary_nested': self.build_sales_summary_nested,
            'product_performance_nested': self.build_product_performance_nested
        }
        
        # Load collection schemas
        self.collection_schemas = self.load_collection_schemas()
        
    def load_collection_schemas(self):
        """Load all collection schemas from support folder"""
        schemas = {}
        support_path = 'support'
        
        for filename in os.listdir(support_path):
            if filename.endswith('.json') and filename != 'transaction_sale.json':
                filepath = os.path.join(support_path, filename)
                try:
                    with open(filepath, 'r') as f:
                        schema = json.load(f)
                        collection_name = schema.get('collection_name')
                        if collection_name:
                            schemas[collection_name] = schema
                            print(f"✅ Loaded schema: {collection_name}")
                except Exception as e:
                    print(f"❌ Error loading {filename}: {e}")
        
        return schemas
    
    def get_date_parsing_pipeline(self):
        """Get standardized date parsing pipeline for mixed date formats"""
        return [
            {
                "$addFields": {
                    # Handle both string (DD/MM/YYYY) and datetime formats
                    "parsed_date": {
                        "$cond": {
                            "if": {"$eq": [{"$type": "$Sales Date"}, "date"]},
                            "then": "$Sales Date",
                            "else": {
                                "$cond": {
                                    "if": {"$eq": [{"$type": "$Sales Date"}, "string"]},
                                    "then": {
                                        "$dateFromString": {
                                            "dateString": "$Sales Date",
                                            "format": "%d/%m/%Y",
                                            "onError": None
                                        }
                                    },
                                    "else": None
                                }
                            }
                        }
                    }
                }
            },
            {
                "$addFields": {
                    # Extract month and year from parsed date, fallback to existing fields
                    "extracted_month": {
                        "$cond": {
                            "if": {"$ne": ["$parsed_date", None]},
                            "then": {"$month": "$parsed_date"},
                            "else": "$month"
                        }
                    },
                    "extracted_year": {
                        "$cond": {
                            "if": {"$ne": ["$parsed_date", None]},
                            "then": {"$year": "$parsed_date"},  
                            "else": "$year"
                        }
                    }
                }
            }
        ]
    
    def connect_db(self):
        """Connect to MongoDB"""
        if self.db is None:
            client = self.mongo_conn.connect()
            if client:
                self.db = self.mongo_conn.get_database()  
                return True
        return self.db is not None
    
    def build_sales_by_location(self):
        """Build sales_by_location collection with proper date parsing"""
        print("🏪 Building sales_by_location collection...")
        
        # Start with date parsing
        pipeline = self.get_date_parsing_pipeline()
        
        # Add numeric conversion and aggregation  
        pipeline.extend([
            {
                "$addFields": {
                    "total_numeric": {
                        "$toDouble": {
                            "$replaceAll": {
                                "input": {"$toString": "$Total"},
                                "find": ",",
                                "replacement": "."
                            }
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$Location Name",
                    "total_sales": {"$sum": "$total_numeric"},
                    "total_transactions": {"$sum": 1},
                    "sales_dates": {"$push": "$Sales Date"},
                    "months": {"$addToSet": "$extracted_month"},
                    "years": {"$addToSet": "$extracted_year"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "location_name": "$_id",
                    "total_sales": {"$round": ["$total_sales", 2]},
                    "total_transactions": 1,
                    "average_transaction": {"$round": [{"$divide": ["$total_sales", "$total_transactions"]}, 2]},
                    "first_sale_date": {"$min": "$sales_dates"},
                    "last_sale_date": {"$max": "$sales_dates"},
                    "active_months": "$months",
                    "active_years": "$years",
                    "last_updated": datetime.now()
                }
            },
            {"$sort": {"total_sales": -1}}
        ])
        
        return self.execute_pipeline_and_save('sales_by_location', pipeline)
    
    def build_sales_by_month(self):
        """Build sales_by_month collection with proper date parsing"""
        print("📅 Building sales_by_month collection...")
        
        month_names = ["", "January", "February", "March", "April", "May", "June",
                      "July", "August", "September", "October", "November", "December"]
        
        # Start with date parsing
        pipeline = self.get_date_parsing_pipeline()
        
        # Add numeric conversion and aggregation
        pipeline.extend([
            {
                "$addFields": {
                    "total_numeric": {
                        "$toDouble": {
                            "$replaceAll": {
                                "input": {"$toString": "$Total"},
                                "find": ",",
                                "replacement": "."
                            }
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": {"year": "$extracted_year", "month": "$extracted_month"},
                    "total_sales": {"$sum": "$total_numeric"},
                    "total_transactions": {"$sum": 1},
                    "locations": {"$addToSet": "$Location Name"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "year": "$_id.year", 
                    "month": "$_id.month",
                    "month_name": {
                        "$arrayElemAt": [month_names, "$_id.month"]
                    },
                    "period": {
                        "$concat": [
                            {"$toString": "$_id.year"},
                            "-",
                            {"$toString": {"$add": [100, "$_id.month"]}},
                        ]
                    },
                    "total_sales": {"$round": ["$total_sales", 2]},
                    "total_transactions": 1,
                    "average_daily_sales": {"$round": [{"$divide": ["$total_sales", 30]}, 2]},
                    "locations_active": "$locations",
                    "top_location": {"$first": "$locations"},
                    "last_updated": datetime.now()
                }
            },
            {"$sort": {"year": 1, "month": 1}}
        ])
        
        return self.execute_pipeline_and_save('sales_by_month', pipeline)
    
    def build_sales_by_location_month(self):
        """Build sales_by_location_month collection with proper date parsing"""
        print("🏪📅 Building sales_by_location_month collection...")
        
        # Start with date parsing
        pipeline = self.get_date_parsing_pipeline()
        
        # Add numeric conversion and aggregation
        pipeline.extend([
            {
                "$addFields": {
                    "total_numeric": {
                        "$toDouble": {
                            "$replaceAll": {
                                "input": {"$toString": "$Total"},
                                "find": ",",
                                "replacement": "."
                            }
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
                    "total_sales": {"$sum": "$total_numeric"},
                    "total_transactions": {"$sum": 1},
                    "customers": {"$addToSet": "$Customer Phone No"},
                    "payment_methods": {"$addToSet": "$Payment Method"},
                    "product_categories": {"$addToSet": "$Product Category Name"},
                    "sales_dates": {"$addToSet": "$Sales Date"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "location_name": "$_id.location",
                    "year": "$_id.year",
                    "month": "$_id.month", 
                    "period": {
                        "$concat": [
                            {"$toString": "$_id.year"},
                            "-",
                            {"$toString": "$_id.month"}
                        ]
                    },
                    "location_period": {
                        "$concat": [
                            "$_id.location",
                            "_",
                            {"$toString": "$_id.year"},
                            "-",
                            {"$toString": "$_id.month"}
                        ]
                    },
                    "total_sales": {"$round": ["$total_sales", 2]},
                    "total_transactions": 1,
                    "average_transaction": {"$round": [{"$divide": ["$total_sales", "$total_transactions"]}, 2]},
                    "unique_customers": {"$size": {"$filter": {"input": "$customers", "cond": {"$ne": ["$$this", None]}}}},
                    "payment_methods": 1,
                    "product_categories": 1,
                    "days_active": {"$size": "$sales_dates"},
                    "last_updated": datetime.now()
                }
            },
            {"$sort": {"location_name": 1, "year": 1, "month": 1}}
        ])
        
        return self.execute_pipeline_and_save('sales_by_location_month', pipeline)
    
    def build_sales_by_product(self):
        """Build sales_by_product collection with proper date parsing"""
        print("🛍️ Building sales_by_product collection...")
        
        # Start with date parsing
        pipeline = self.get_date_parsing_pipeline()
        
        # Add numeric conversion and aggregation
        pipeline.extend([
            {
                "$addFields": {
                    "gross_sales_numeric": {
                        "$toDouble": {
                            "$replaceAll": {
                                "input": {"$toString": "$Gross Sales"},
                                "find": ",",
                                "replacement": "."
                            }
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "product": "$Product Name",
                        "category": "$Product Category Name"
                    },
                    "total_quantity_sold": {"$sum": "$Product qty"},
                    "total_revenue": {"$sum": "$gross_sales_numeric"},
                    "total_transactions": {"$sum": 1},
                    "prices": {"$push": "$Price"},
                    "locations": {"$addToSet": "$Location Name"},
                    "months": {"$addToSet": "$extracted_month"},
                    "years": {"$addToSet": "$extracted_year"},
                    "sales_dates": {"$push": "$Sales Date"},
                    "location_sales": {
                        "$push": {
                            "location": "$Location Name",
                            "sales": "$gross_sales_numeric"
                        }
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "product_name": "$_id.product",
                    "product_category": "$_id.category",
                    "total_quantity_sold": 1,
                    "total_revenue": {"$round": ["$total_revenue", 2]},
                    "total_transactions": 1,
                    "average_price": {"$round": [{"$avg": "$prices"}, 2]},
                    "locations_sold": "$locations",
                    "months_active": "$months",
                    "years_active": "$years",
                    "best_performing_location": {"$first": "$locations"},
                    "last_sale_date": {"$max": "$sales_dates"},
                    "last_updated": datetime.now()
                }
            },
            {"$sort": {"total_revenue": -1}}
        ])
        
        return self.execute_pipeline_and_save('sales_by_product', pipeline)
    
    def build_sales_by_payment_method(self):
        """Build sales_by_payment_method collection with proper date parsing"""
        print("💳 Building sales_by_payment_method collection...")
        
        # Start with date parsing
        pipeline = self.get_date_parsing_pipeline()
        
        # Add numeric conversion and aggregation (calculate percentages in a separate pipeline stage)
        pipeline.extend([
            {
                "$addFields": {
                    "total_numeric": {
                        "$toDouble": {
                            "$replaceAll": {
                                "input": {"$toString": "$Total"},
                                "find": ",",
                                "replacement": "."
                            }
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$Payment Method",
                    "total_sales": {"$sum": "$total_numeric"},
                    "total_transactions": {"$sum": 1},
                    "locations": {"$addToSet": "$Location Name"},
                    "months": {"$addToSet": "$extracted_month"},
                    "years": {"$addToSet": "$extracted_year"},
                    "sales_dates": {"$push": "$Sales Date"},
                    "monthly_sales": {
                        "$push": {
                            "month": "$extracted_month",
                            "year": "$extracted_year", 
                            "sales": "$total_numeric"
                        }
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "payment_method": "$_id",
                    "total_sales": {"$round": ["$total_sales", 2]},
                    "total_transactions": 1,
                    "average_transaction": {"$round": [{"$divide": ["$total_sales", "$total_transactions"]}, 2]},
                    "percentage_of_total": 0,  # Will calculate this after aggregation
                    "locations_used": "$locations",
                    "months_active": "$months", 
                    "years_active": "$years",
                    "peak_usage_month": {"$toString": {"$max": "$months"}},
                    "last_used_date": {"$max": "$sales_dates"},
                    "last_updated": datetime.now()
                }
            },
            {"$sort": {"total_sales": -1}}
        ])
        
        return self.execute_pipeline_and_save('sales_by_payment_method', pipeline)
    
    def build_sales_summary_nested(self):
        """Build hierarchical sales summary with proper date parsing"""
        print("📊 Building sales_summary_nested collection...")
        
        # Start with date parsing
        pipeline = self.get_date_parsing_pipeline()
        
        # Add numeric conversion and nested aggregation
        pipeline.extend([
            {
                "$addFields": {
                    "total_numeric": {
                        "$toDouble": {
                            "$replaceAll": {
                                "input": {"$toString": "$Total"},
                                "find": ",",
                                "replacement": "."
                            }
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
                    "monthly_sales": {"$sum": "$total_numeric"},
                    "monthly_transactions": {"$sum": 1},
                    "products_sold": {"$addToSet": "$Product Name"},
                    "categories": {"$addToSet": "$Product Category Name"},
                    "payment_methods": {"$addToSet": "$Payment Method"}
                }
            },
            {
                "$group": {
                    "_id": "$_id.location",
                    "total_sales": {"$sum": "$monthly_sales"},
                    "total_transactions": {"$sum": "$monthly_transactions"},
                    "monthly_breakdown": {
                        "$push": {
                            "year": "$_id.year",
                            "month": "$_id.month",
                            "period": {
                                "$concat": [
                                    {"$toString": "$_id.year"},
                                    "-",
                                    {"$toString": "$_id.month"}
                                ]
                            },
                            "sales": "$monthly_sales",
                            "transactions": "$monthly_transactions",
                            "products_sold": "$products_sold",
                            "categories": "$categories",
                            "payment_methods": "$payment_methods"
                        }
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "location_name": "$_id",
                    "total_sales": {"$round": ["$total_sales", 2]},
                    "total_transactions": 1,
                    "average_transaction": {"$round": [{"$divide": ["$total_sales", "$total_transactions"]}, 2]},
                    "monthly_breakdown": 1,
                    "active_months": {"$size": "$monthly_breakdown"},
                    "last_updated": datetime.now()
                }
            },
            {"$sort": {"total_sales": -1}}
        ]
        
        return self.execute_pipeline_and_save('sales_summary_nested', pipeline)
    
    def build_product_performance_nested(self):
        """Build hierarchical product performance with proper date parsing"""
        print("🛍️ Building product_performance_nested collection...")
        
        # Start with date parsing
        pipeline = self.get_date_parsing_pipeline()
        
        # Add numeric conversion and grouped sub-documents
        pipeline.extend([
            {
                "$addFields": {
                    "gross_sales_numeric": {
                        "$toDouble": {
                            "$replaceAll": {
                                "input": {"$toString": "$Gross Sales"},
                                "find": ",",
                                "replacement": "."
                            }
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "product": "$Product Name",
                        "category": "$Product Category Name",
                        "location": "$Location Name",
                        "year": "$extracted_year",
                        "month": "$extracted_month"
                    },
                    "location_month_revenue": {"$sum": "$gross_sales_numeric"},
                    "location_month_quantity": {"$sum": "$Product qty"},
                    "location_month_transactions": {"$sum": 1},
                    "avg_price": {"$avg": "$Price"}
                }
            },
            {
                "$group": {
                    "_id": {
                        "product": "$_id.product",
                        "category": "$_id.category"
                    },
                    "total_revenue": {"$sum": "$location_month_revenue"},
                    "total_quantity": {"$sum": "$location_month_quantity"},
                    "total_transactions": {"$sum": "$location_month_transactions"},
                    "average_price": {"$avg": "$avg_price"},
                    "performance_breakdown": {
                        "$push": {
                            "location": "$_id.location",
                            "year": "$_id.year", 
                            "month": "$_id.month",
                            "period": {
                                "$concat": [
                                    {"$toString": "$_id.year"},
                                    "-",
                                    {"$toString": "$_id.month"}
                                ]
                            },
                            "location_period": {
                                "$concat": [
                                    "$_id.location",
                                    "_",
                                    {"$toString": "$_id.year"},
                                    "-",
                                    {"$toString": "$_id.month"}
                                ]
                            },
                            "revenue": "$location_month_revenue",
                            "quantity": "$location_month_quantity",
                            "transactions": "$location_month_transactions"
                        }
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "product_name": "$_id.product",
                    "product_category": "$_id.category",
                    "total_revenue": {"$round": ["$total_revenue", 2]},
                    "total_quantity": 1,
                    "total_transactions": 1,
                    "average_price": {"$round": ["$average_price", 2]},
                    "performance_breakdown": 1,
                    "locations_count": {
                        "$size": {
                            "$setUnion": [
                                {"$map": {"input": "$performance_breakdown", "as": "item", "in": "$$item.location"}},
                                []
                            ]
                        }
                    },
                    "months_active": {
                        "$size": {
                            "$setUnion": [
                                {"$map": {"input": "$performance_breakdown", "as": "item", "in": "$$item.period"}},
                                []
                            ]
                        }
                    },
                    "last_updated": datetime.now()
                }
            },
            {"$sort": {"total_revenue": -1}}
        ])
        
        return self.execute_pipeline_and_save('product_performance_nested', pipeline)
    
    def execute_pipeline_and_save(self, collection_name, pipeline):
        """Execute aggregation pipeline and save to collection"""
        try:
            # Ensure database connection is active
            if self.db is None:
                if not self.connect_db():
                    print(f"❌ Cannot connect to database for {collection_name}")
                    return False
            
            # Execute aggregation
            source_collection = self.db['transaction_sales']
            results = list(source_collection.aggregate(pipeline))
            
            if not results:
                print(f"⚠️  No data found for {collection_name}")
                return False
            
            # Drop existing collection
            self.db[collection_name].drop()
            
            # Insert new data
            target_collection = self.db[collection_name]
            insert_result = target_collection.insert_many(results)
            
            # Create indexes
            self.create_indexes(collection_name)
            
            print(f"✅ Created {collection_name}: {len(insert_result.inserted_ids)} records")
            return True
            
        except Exception as e:
            print(f"❌ Error building {collection_name}: {e}")
            # Try to reconnect if connection was lost
            if "Cannot use MongoClient after close" in str(e):
                print("🔄 Reconnecting to database...")
                self.db = None
                if self.connect_db():
                    print("✅ Reconnection successful, retrying operation...")
                    return self.execute_pipeline_and_save(collection_name, pipeline)
            return False
    
    def create_indexes(self, collection_name):
        """Create appropriate indexes for collection"""
        try:
            collection = self.db[collection_name]
            
            if collection_name == 'sales_by_location':
                collection.create_index("location_name")
                
            elif collection_name == 'sales_by_month':
                collection.create_index([("year", 1), ("month", 1)])
                collection.create_index("period")
                
            elif collection_name == 'sales_by_location_month':
                collection.create_index("location_name")
                collection.create_index([("year", 1), ("month", 1)])
                collection.create_index("location_period")
                
            elif collection_name == 'sales_by_product':
                collection.create_index("product_name")
                collection.create_index("product_category")
                
            elif collection_name == 'sales_by_payment_method':
                collection.create_index("payment_method")
                
            print(f"📈 Created indexes for {collection_name}")
            
        except Exception as e:
            print(f"⚠️  Index creation warning for {collection_name}: {e}")
    
    def suggest_collection_for_query(self, user_query):
        """Suggest which collection to use based on user query"""
        query_lower = user_query.lower()
        
        # Define keywords for each collection with different weights
        collection_keywords = {
            'sales_by_location': {
                'primary': ['lokasi', 'location', 'toko', 'store'], # weight 3
                'secondary': ['cabang', 'branch', 'per lokasi', 'by location'] # weight 2
            },
            'sales_by_month': {
                'primary': ['bulan', 'month', 'bulanan', 'monthly'],
                'secondary': ['trend', 'tahun', 'year', 'per bulan', 'by month']
            },
            'sales_by_location_month': {
                'primary': ['per lokasi per bulan', 'location month', 'lokasi bulan', 'by location by month'],
                'secondary': ['toko bulan', 'store month', 'lokasi per bulan', 'location and month']
            },
            'sales_by_product': {
                'primary': ['produk', 'product', 'barang', 'item'],
                'secondary': ['kategori', 'category', 'per produk', 'by product']
            },
            'sales_by_payment_method': {
                'primary': ['payment', 'pembayaran', 'bayar'],
                'secondary': ['cash', 'qris', 'card', 'metode', 'method']
            }
        }
        
        # Score each collection with weighted keywords
        scores = {}
        for collection, keyword_groups in collection_keywords.items():
            score = 0
            # Primary keywords worth 3 points
            for keyword in keyword_groups['primary']:
                if keyword in query_lower:
                    score += 3
            # Secondary keywords worth 2 points  
            for keyword in keyword_groups['secondary']:
                if keyword in query_lower:
                    score += 2
            
            if score > 0:
                scores[collection] = score
        
        # Return best match with tie-breaking preference for more specific collections
        if scores:
            max_score = max(scores.values())
            tied_collections = [coll for coll, score in scores.items() if score == max_score]
            
            # If there's a tie, prefer more specific collections (longer name = more specific)
            if len(tied_collections) > 1:
                best_collection = max(tied_collections, key=len)
            else:
                best_collection = tied_collections[0]
            
            return best_collection, scores[best_collection]
        
        return None, 0
    
    def build_all_collections(self):
        """Build all optimized collections"""
        print("🏗️  Building All Optimized Collections")
        print("=" * 50)
        
        if not self.connect_db():
            print("❌ Cannot connect to database")
            return False
        
        success_count = 0
        total_count = len(self.collection_builders)
        
        try:
            for collection_name, builder_func in self.collection_builders.items():
                print(f"\n📊 Building {collection_name}...")
                # Ensure connection is still active before each build
                if self.db is None:
                    if not self.connect_db():
                        print(f"❌ Cannot reconnect to database for {collection_name}")
                        continue
                
                if builder_func():
                    success_count += 1
                else:
                    print(f"❌ Failed to build {collection_name}")
            
            print(f"\n" + "=" * 50)
            print(f"📈 Summary: {success_count}/{total_count} collections built successfully")
            
            if success_count == total_count:
                print("🎉 All optimized collections created successfully!")
                
        finally:
            # Always disconnect at the end
            if hasattr(self, 'mongo_conn') and self.mongo_conn:
                self.mongo_conn.disconnect()
        
        return success_count == total_count
    
    def build_single_collection(self, collection_name):
        """Build a single optimized collection"""
        if not self.connect_db():
            print("❌ Cannot connect to database")
            return False
        
        if collection_name not in self.collection_builders:
            print(f"❌ Unknown collection: {collection_name}")
            print(f"Available collections: {list(self.collection_builders.keys())}")
            return False
        
        try:
            print(f"🔨 Building {collection_name}...")
            success = self.collection_builders[collection_name]()
            return success
        finally:
            # Always disconnect at the end
            if hasattr(self, 'mongo_conn') and self.mongo_conn:
                self.mongo_conn.disconnect()

def main():
    print("🏗️  Optimized Collection Builder")
    print("=" * 40)
    
    builder = OptimizedCollectionBuilder()
    
    # Build all collections
    builder.build_all_collections()
    
    # Test suggestion system
    print(f"\n🧠 Testing Collection Suggestion System:")
    test_queries = [
        "tampilkan penjualan per lokasi",
        "sales trend by month", 
        "product performance analysis",
        "payment method comparison",
        "penjualan per lokasi per bulan"
    ]
    
    for query in test_queries:
        suggested, score = builder.suggest_collection_for_query(query)
        print(f"Query: '{query}' -> Suggested: {suggested} (score: {score})")

if __name__ == "__main__":
    main()