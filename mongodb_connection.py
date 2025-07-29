import pymongo
import paramiko
from sshtunnel import SSHTunnelForwarder
import os
import json
import requests
from datetime import datetime, timedelta
import random

class MongoDBSSHConnection:
    def __init__(self):
        self.ssh_host = '103.93.56.51'
        self.ssh_port = 22
        self.ssh_username = 'ubuntu'
        self.ssh_key_path = '/Users/dedykurniawan/Downloads/pemFile/teh.pem'
        self.mongo_host = 'localhost'
        self.mongo_port = 27017
        self.db_name = 'esteh'  # Changed to use the database with 3.6M records
        self.tunnel = None
        self.client = None
        self.OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY') or "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"
        
    def connect(self):
        try:
            # Create SSH tunnel
            self.tunnel = SSHTunnelForwarder(
                (self.ssh_host, self.ssh_port),
                ssh_username=self.ssh_username,
                ssh_pkey=self.ssh_key_path,
                remote_bind_address=(self.mongo_host, self.mongo_port),
                local_bind_address=('localhost', 0)  # Use any available local port
            )
            
            self.tunnel.start()
            local_port = self.tunnel.local_bind_port
            
            # Connect to MongoDB through the tunnel
            self.client = pymongo.MongoClient(f'mongodb://localhost:{local_port}/')
            
            # Test connection
            self.client.admin.command('ping')
            print(f"Successfully connected to MongoDB via SSH tunnel on port {local_port}")
            
            return self.client
            
        except Exception as e:
            print(f"Connection failed: {e}")
            if self.tunnel:
                self.tunnel.stop()
            return None
    
    def disconnect(self):
        if self.client:
            self.client.close()
        if self.tunnel:
            self.tunnel.stop()
        print("Disconnected from MongoDB")
    
    def get_database(self, db_name=None):
        if not self.client:
            raise Exception("No active connection. Call connect() first.")
        
        db_name = db_name or self.db_name
        return self.client[db_name]
    
    def generate_sample_data_with_claude(self, num_records=10):
        if not self.OPENROUTER_API_KEY:
            raise Exception("OPENROUTER_API_KEY not found in environment variables")
        
        # Load schema
        with open('support/transaction_sale.json', 'r') as f:
            schema = json.load(f)
        
        prompt = f"""
        Generate {num_records} realistic sample records for a transaction_sale collection based on this schema:
        {json.dumps(schema, indent=2)}
        
        Requirements:
        - Generate realistic Indonesian restaurant/cafe transaction data
        - Use Indonesian location names (like Jakarta, Bandung, Surabaya)
        - Product names should be tea/coffee/food items in Indonesian
        - Use realistic prices in Indonesian Rupiah (thousands)
        - Sales dates should be recent (within last 30 days)
        - Include various payment methods (Cash, QRIS, Card)
        - Return only valid JSON array format
        - Each record should be a complete transaction item
        
        Return format: [{{record1}}, {{record2}}, ...]
        """
        
        try:
            response = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.OPENROUTER_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "anthropic/claude-3.5-sonnet",
                    "messages": [{"role": "user", "content": prompt}]
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content']
                
                # Extract JSON from response
                start = content.find('[')
                end = content.rfind(']') + 1
                json_str = content[start:end]
                
                return json.loads(json_str)
            else:
                raise Exception(f"OpenRouter API error: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"Error generating data with Claude: {e}")
            return None
    
    def insert_transaction_data(self, data):
        if not self.client:
            raise Exception("No active connection. Call connect() first.")
        
        db = self.get_database()
        collection = db['transaction_sale']
        
        if isinstance(data, list):
            result = collection.insert_many(data)
            print(f"Inserted {len(result.inserted_ids)} records")
            return result
        else:
            result = collection.insert_one(data)
            print(f"Inserted 1 record with ID: {result.inserted_id}")
            return result
    
    def create_aggregation_pipeline(self):
        """
        Creates various aggregation pipelines for transaction_sale collection
        """
        pipelines = {
            # Sales summary by location
            "sales_by_location": [
                {
                    "$group": {
                        "_id": "$Location Name",
                        "total_sales": {"$sum": {"$toDouble": "$Total"}},
                        "total_transactions": {"$sum": 1},
                        "avg_transaction": {"$avg": {"$toDouble": "$Total"}}
                    }
                },
                {"$sort": {"total_sales": -1}}
            ],
            
            # Daily sales trend
            "daily_sales": [
                {
                    "$addFields": {
                        "sale_date": {
                            "$dateFromString": {
                                "dateString": "$Sales Date",
                                "format": "%d/%m/%Y"
                            }
                        }
                    }
                },
                {
                    "$group": {
                        "_id": "$sale_date",
                        "daily_total": {"$sum": {"$toDouble": "$Total"}},
                        "transaction_count": {"$sum": 1}
                    }
                },
                {"$sort": {"_id": 1}}
            ],
            
            # Product performance
            "product_performance": [
                {
                    "$group": {
                        "_id": {
                            "category": "$Product Category Name",
                            "product": "$Product Name"
                        },
                        "total_qty": {"$sum": "$Product qty"},
                        "total_revenue": {"$sum": {"$toDouble": "$Gross Sales"}},
                        "avg_price": {"$avg": "$Price"}
                    }
                },
                {"$sort": {"total_revenue": -1}}
            ],
            
            # Payment method analysis
            "payment_methods": [
                {
                    "$group": {
                        "_id": "$Payment Method",
                        "count": {"$sum": 1},
                        "total_amount": {"$sum": {"$toDouble": "$Total"}},
                        "avg_amount": {"$avg": {"$toDouble": "$Total"}}
                    }
                },
                {"$sort": {"total_amount": -1}}
            ],
            
            # Hourly sales pattern
            "hourly_pattern": [
                {
                    "$addFields": {
                        "hour": {
                            "$toInt": {
                                "$substr": ["$Sales Time", 0, 2]
                            }
                        }
                    }
                },
                {
                    "$group": {
                        "_id": "$hour",
                        "transactions": {"$sum": 1},
                        "total_sales": {"$sum": {"$toDouble": "$Total"}}
                    }
                },
                {"$sort": {"_id": 1}}
            ]
        }
        
        return pipelines
    
    def run_aggregation(self, pipeline_name):
        if not self.client:
            raise Exception("No active connection. Call connect() first.")
        
        pipelines = self.create_aggregation_pipeline()
        if pipeline_name not in pipelines:
            raise Exception(f"Pipeline '{pipeline_name}' not found. Available: {list(pipelines.keys())}")
        
        db = self.get_database()
        collection = db['transaction_sale']
        
        result = list(collection.aggregate(pipelines[pipeline_name]))
        return result

# Usage example
if __name__ == "__main__":
    mongo_conn = MongoDBSSHConnection()
    
    try:
        client = mongo_conn.connect()
        if client:
            # Get database
            db = mongo_conn.get_database()
            
            # List collections
            collections = db.list_collection_names()
            print(f"Collections in {mongo_conn.db_name}: {collections}")
            
            # Generate sample data using Claude (uncomment to use)
            print("\n=== Generating Sample Data with Claude ===")
            sample_data = mongo_conn.generate_sample_data_with_claude(5)
            if sample_data:
                print(f"Generated {len(sample_data)} sample records")
                
                # Insert sample data
                mongo_conn.insert_transaction_data(sample_data)
                
                # Run aggregation pipelines
                print("\n=== Running Aggregations ===")
                
                # Sales by location
                print("\n1. Sales by Location:")
                location_sales = mongo_conn.run_aggregation("sales_by_location")
                for item in location_sales[:3]:  # Show top 3
                    print(f"  {item['_id']}: Rp {item['total_sales']:,.0f} ({item['total_transactions']} transactions)")
                
                # Payment methods
                print("\n2. Payment Methods:")
                payment_methods = mongo_conn.run_aggregation("payment_methods")
                for item in payment_methods:
                    print(f"  {item['_id']}: Rp {item['total_amount']:,.0f} ({item['count']} transactions)")
                
                # Product performance
                print("\n3. Top Products:")
                products = mongo_conn.run_aggregation("product_performance")
                for item in products[:3]:  # Show top 3
                    print(f"  {item['_id']['product']}: Rp {item['total_revenue']:,.0f} ({item['total_qty']} qty)")
            
            # Example: Query existing data
            collection = db['transaction_sale']
            total_records = collection.count_documents({})
            print(f"\nTotal records in transaction_sale: {total_records}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        mongo_conn.disconnect()