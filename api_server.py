#!/usr/bin/env python3

from flask import Flask, request, jsonify
from flask_restx import Api, Resource, fields, Namespace
import requests
import json
import os
from mongodb_connection import MongoDBSSHConnection
from collection_builder import OptimizedCollectionBuilder
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
api = Api(
    app, 
    version='1.0', 
    title='Transaction Sale Aggregate API',
    description='API untuk mengeksekusi agregasi MongoDB dengan bantuan AI translation dan generation',
    doc='/docs'
)

# Namespaces
ns_aggregate = Namespace('aggregate', description='MongoDB Aggregation Operations')
api.add_namespace(ns_aggregate)

# Models for Swagger documentation
aggregate_request_model = api.model('AggregateRequest', {
    'command': fields.String(required=True, description='User command in Indonesian or English', 
                            example='tampilkan penjualan per lokasi bulan ini'),
    'collection': fields.String(required=False, description='MongoDB collection name', 
                               default='transaction_sale'),
    'limit': fields.Integer(required=False, description='Limit results (optional - if not specified, returns all results)')
})

aggregate_response_model = api.model('AggregateResponse', {
    'success': fields.Boolean(description='Operation success status'),
    'original_command': fields.String(description='Original user command'),
    'translated_command': fields.String(description='English translated command'),
    'generated_pipeline': fields.Raw(description='Generated MongoDB aggregation pipeline'),
    'results': fields.Raw(description='Aggregation results from MongoDB'),
    'description': fields.String(description='Detailed business analytics description by Mixtral'),
    'total_results': fields.Integer(description='Total number of results'),
    'execution_time': fields.Float(description='Execution time in seconds'),
    'collection_used': fields.String(description='MongoDB collection used'),
    'documents_in_collection': fields.Integer(description='Total documents in collection'),
    'error': fields.String(description='Error message if any')
})

error_model = api.model('ErrorResponse', {
    'success': fields.Boolean(default=False),
    'error': fields.String(description='Error message'),
    'details': fields.String(description='Detailed error information')
})

class AIService:
    def __init__(self):
        self.openrouter_api_key = os.getenv('OPENROUTER_API_KEY') or "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"
        print(f"🔑 AIService initialized with API key: {self.openrouter_api_key[:15]}...")
        
        self.headers = {
            "Authorization": f"Bearer {self.openrouter_api_key}",
            "Content-Type": "application/json"
        }
    
    def analyze_results_with_mixtral(self, user_command, results, pipeline):
        """Analyze aggregation results and create detailed analytics description using Mixtral"""
        analysis_prompt = f"""
        Anda adalah seorang ahli analisis bisnis. Analisis hasil agregasi MongoDB berikut dan berikan wawasan bisnis yang mendalam.
        
        Permintaan pengguna: "{user_command}"
        
        Pipeline agregasi yang digunakan: {json.dumps(pipeline, indent=2)}
        
        Hasil: {json.dumps(results, indent=2, default=str)}
        
        Berikan analisis bisnis yang komprehensif meliputi:
        1. Temuan utama dan tren
        2. Performa terbaik dan metriknya
        3. Wawasan dan implikasi bisnis
        4. Rekomendasi berdasarkan data
        5. Pola atau anomali yang menonjol
        
        Tulis dalam nada profesional dan analitis yang sesuai untuk stakeholder bisnis.
        Buat ringkas namun informatif (maksimal 3-5 paragraf).
        Gunakan bahasa Indonesia yang baik dan benar.
        """
        
        try:
            response = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=self.headers,
                json={
                    "model": "mistralai/mixtral-8x7b-instruct",
                    "messages": [{"role": "user", "content": analysis_prompt}],
                    "temperature": 0.3
                },
                timeout=45
            )
            
            if response.status_code == 200:
                result = response.json()
                return result['choices'][0]['message']['content'].strip()
            else:
                logger.error(f"Mixtral analysis API error: {response.status_code} - {response.text}")
                return "Analysis unavailable due to API error."
                
        except Exception as e:
            logger.error(f"Error analyzing results with Mixtral: {e}")
            return "Analysis unavailable due to processing error."
    
    def translate_with_mixtral(self, user_command):
        """Translate user command to English using Mixtral"""
        prompt = f"""
        Translate the following command to clear English for database/analytics queries. 
        Be precise and don't add information that wasn't in the original command.
        
        Important guidelines:
        - "bulan juni" = "June" or "month of June", not "per month"
        - "per lokasi" = "by location" or "per location"
        - Don't add years unless specifically mentioned
        - Keep it simple and accurate
        
        Command: "{user_command}"
        
        Return only the translated command, nothing else.
        """
        
        try:
            response = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=self.headers,
                json={
                    "model": "mistralai/mixtral-8x7b-instruct",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                return result['choices'][0]['message']['content'].strip()
            else:
                logger.error(f"Mixtral API error: {response.status_code} - {response.text}")
                return user_command  # Fallback to original
                
        except Exception as e:
            logger.error(f"Error translating with Mixtral: {e}")
            return user_command  # Fallback to original
    
    def get_pipeline_template(self, translated_command, collection_name):
        """Get predefined pipeline templates for common queries"""
        command_lower = translated_command.lower()
        
        # Template for product categories by location for June using sales_by_location_month
        if collection_name == "sales_by_location_month":
            if ("product" in command_lower or "category" in command_lower or "kategori" in command_lower) and \
               ("location" in command_lower or "lokasi" in command_lower) and \
               ("june" in command_lower or "juni" in command_lower):
                return [
                    {"$match": {"month": 6}},
                    {"$unwind": "$product_categories"},
                    {"$group": {
                        "_id": {
                            "location": "$location_name", 
                            "category": "$product_categories"
                        }, 
                        "total_sales": {"$first": "$total_sales"}
                    }},
                    {"$sort": {"total_sales": -1}},
                    {"$group": {
                        "_id": "$_id.location", 
                        "top_categories": {
                            "$push": {
                                "category": "$_id.category", 
                                "sales": "$total_sales"
                            }
                        }
                    }},
                    {"$project": {
                        "location": "$_id", 
                        "top_categories": {"$slice": ["$top_categories", 10]}, 
                        "_id": 0
                    }},
                    {"$sort": {"location": 1}}
                ]
        
        # Template for top products from top locations using product_performance_nested
        if collection_name == "product_performance_nested":
            if ("product" in command_lower or "produk" in command_lower) and \
               ("location" in command_lower or "lokasi" in command_lower) and \
               ("terbanyak" in command_lower or "terbesar" in command_lower or "top" in command_lower):
                return [
                    {"$sort": {"total_revenue": -1}},
                    {"$limit": 100},  # Get top 100 products first
                    {"$unwind": "$performance_breakdown"},
                    {"$group": {
                        "_id": "$performance_breakdown.location",
                        "location_total": {"$sum": "$performance_breakdown.revenue"},
                        "products": {
                            "$push": {
                                "product_name": "$product_name",
                                "product_category": "$product_category", 
                                "revenue": "$performance_breakdown.revenue",
                                "quantity": "$performance_breakdown.quantity"
                            }
                        }
                    }},
                    {"$sort": {"location_total": -1}},
                    {"$limit": 10},  # Top 10 locations by revenue
                    {"$project": {
                        "location": "$_id",
                        "location_total": 1,
                        "top_products": {"$slice": [{"$sortArray": {"input": "$products", "sortBy": {"revenue": -1}}}, 10]},
                        "_id": 0
                    }},
                    {"$sort": {"location_total": -1}}
                ]
        
        return None

    def generate_pipeline_with_claude(self, translated_command, collection_schema, collection_name="transaction_sale"):
        """Generate MongoDB aggregation pipeline using Claude"""
        
        # Check for predefined pipeline templates
        pipeline_template = self.get_pipeline_template(translated_command, collection_name)
        if pipeline_template:
            return pipeline_template
        
        # Determine if this is an optimized collection
        is_optimized = collection_name != "transaction_sales"
        
        if is_optimized:
            prompt = f"""
            You are a MongoDB aggregation expert. Generate a MongoDB aggregation pipeline for a PRE-AGGREGATED collection.
            
            Request: "{translated_command}"
            
            Collection: {collection_name} (This is a pre-aggregated optimized collection)
            Schema: {json.dumps(collection_schema, indent=2)}
            
            CRITICAL - This collection contains PRE-AGGREGATED data:
            1. Data is already grouped and summarized
            2. Use the exact field names from the schema above
            3. For sales_by_location: use "location_name", "total_sales", "total_transactions", "average_transaction"
            4. For sales_by_month: use "month", "year", "total_sales", "total_transactions"  
            5. For sales_by_payment_method: use "payment_method", "total_sales", "total_transactions"
            6. For sales_by_product: use "product_name", "total_revenue", "total_quantity_sold"
            7. For sales_by_location_month: use "location_name", "month", "year", "total_sales"
            
            Simple pipeline examples for optimized collections:
            - List all locations: [{{"$project": {{"location_name": 1, "total_sales": 1, "total_transactions": 1}}}}, {{"$sort": {{"total_sales": -1}}}}]
            - Filter by month: [{{"$match": {{"month": 6}}}}, {{"$sort": {{"total_sales": -1}}}}]
            - Top performers: [{{"$sort": {{"total_sales": -1}}}}, {{"$limit": 5}}]
            
            Return ONLY a valid JSON array. Keep it simple since data is already aggregated.
            """
        else:
            prompt = f"""
            You are a MongoDB aggregation expert. Generate a MongoDB aggregation pipeline based on this request:
            
            Request: "{translated_command}"
            
            Collection schema for transaction_sale:
            {json.dumps(collection_schema, indent=2)}
            
            Important guidelines:
            1. Return ONLY a valid JSON array for the aggregation pipeline
            2. Use appropriate MongoDB operators ($group, $match, $sort, $project, etc.)
            3. Handle date fields properly (Sales Date format: DD/MM/YYYY, Sales Time format: HH:MM:SS)
            4. Convert string numbers to actual numbers using $toDouble when needed
            5. Use meaningful field names in results
            6. Only add $limit stage if specifically requested in the user query
            7. IMPORTANT: The sample data is from 2024, not 2025. If filtering by year, use 2024 or don't filter by year at all
            8. For month requests like "juni" or "June", filter for month 06
            9. The collection has 'month' and 'year' fields (integers) extracted from Sales Date that you can use for easier filtering
            
            Examples of good pipelines:
            - For sales by location: [{{"$group": {{"_id": "$Location Name", "total_sales": {{"$sum": {{"$toDouble": "$Total"}}}}, "count": {{"$sum": 1}}}}}}, {{"$sort": {{"total_sales": -1}}}}]
            - For June sales: [{{"$match": {{"month": 6}}}}, {{"$group": {{"_id": "$Location Name", "total": {{"$sum": {{"$toDouble": "$Total"}}}}}}}}]
            - For 2024 sales: [{{"$match": {{"year": 2024}}}}, {{"$group": {{"_id": "$Location Name", "total": {{"$sum": {{"$toDouble": "$Total"}}}}}}}}]
            
            Return format: [pipeline_stage1, pipeline_stage2, ...]
            """
        
        try:
            response = requests.post(
                "https://openrouter.ai/api/v1/chat/completions",
                headers=self.headers,
                json={
                    "model": "anthropic/claude-3.5-sonnet",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1
                },
                timeout=60
            )
            
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content'].strip()
                
                # Try multiple ways to extract JSON array
                try:
                    # Method 1: Look for JSON array
                    start = content.find('[')
                    end = content.rfind(']') + 1
                    
                    if start != -1 and end > start:
                        json_str = content[start:end]
                        # Clean up common JSON issues
                        json_str = json_str.replace('\n', ' ').replace('\r', '')
                        return json.loads(json_str)
                    
                    # Method 2: Try to parse entire content as JSON
                    return json.loads(content)
                    
                except json.JSONDecodeError as e:
                    # Method 3: Try to extract and fix common issues
                    print(f"JSON decode error: {e}")
                    print(f"Content received from Claude: {content}")
                    
                    # Look for pipeline patterns and try to fix them
                    if '$group' in content or '$match' in content:
                        # Try to extract and fix basic pipeline
                        import re
                        # This is a simple fallback - in production you'd want more robust parsing
                        raise ValueError(f"Could not parse Claude response as valid JSON: {e}")
                    else:
                        raise ValueError("No valid JSON array found in Claude response")
                    
            else:
                raise Exception(f"Claude API error: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Error generating pipeline with Claude: {e}")
            raise

# Initialize services (will be initialized on first request if needed)
ai_service = None
mongo_conn = None

def get_ai_service():
    global ai_service
    if ai_service is None:
        ai_service = AIService()
    return ai_service

def get_mongo_connection():
    global mongo_conn
    if mongo_conn is None:
        mongo_conn = MongoDBSSHConnection()
    return mongo_conn

@ns_aggregate.route('/execute')
class AggregateExecute(Resource):
    
    def create_intelligent_alternative(self, collection, user_command, translated_command, ai_svc, schema):
        """Create intelligent alternative when no data found"""
        try:
            print("🧠 Step 1: Analyzing existing data structure...")
            
            # Get sample data to understand what's available
            sample_pipeline = [
                {"$sample": {"size": 5}},
                {"$project": {
                    "Location Name": 1,
                    "Sales Date": 1,
                    "month": 1, 
                    "year": 2,
                    "Product Category Name": 1,
                    "Product Name": 1,
                    "Payment Method": 1,
                    "Total": 1
                }}
            ]
            sample_data = list(collection.aggregate(sample_pipeline))
            
            # Get data summary
            summary_pipeline = [
                {"$group": {
                    "_id": None,
                    "date_range": {
                        "$push": "$Sales Date"
                    },
                    "locations": {
                        "$addToSet": "$Location Name"
                    },
                    "months": {
                        "$addToSet": "$month"
                    },
                    "years": {
                        "$addToSet": "$year"
                    },
                    "categories": {
                        "$addToSet": "$Product Category Name"
                    },
                    "payment_methods": {
                        "$addToSet": "$Payment Method"
                    },
                    "total_documents": {
                        "$sum": 1
                    }
                }},
                {"$project": {
                    "_id": 0,
                    "locations": 1,
                    "months": 1,
                    "years": 1,
                    "categories": 1,
                    "payment_methods": 1,
                    "total_documents": 1,
                    "earliest_date": {"$min": "$date_range"},
                    "latest_date": {"$max": "$date_range"}
                }}
            ]
            
            data_summary = list(collection.aggregate(summary_pipeline))[0]
            print(f"📊 Data summary: {data_summary}")
            
            print("🤖 Step 2: Asking Claude for alternative solution...")
            
            alternative_prompt = f'''
            The user requested: "{user_command}" (translated: "{translated_command}")
            
            However, this query returned no results from the transaction_sale collection.
            
            Here's what data is actually available:
            - Total documents: {data_summary['total_documents']}
            - Available locations: {data_summary['locations']}
            - Available months: {data_summary['months']}
            - Available years: {data_summary['years']}
            - Available categories: {data_summary['categories']}
            - Available payment methods: {data_summary['payment_methods']}
            - Date range: {data_summary.get('earliest_date', 'N/A')} to {data_summary.get('latest_date', 'N/A')}
            
            Sample data: {sample_data[:3]}
            
            Please create an alternative MongoDB aggregation pipeline that:
            1. Uses the actually available data
            2. Provides the closest possible answer to the user's intent
            3. Is meaningful and useful
            4. Returns actual results
            
            For example:
            - If they asked for June but June data doesn't exist, show available months
            - If they asked for a specific location that doesn't exist, show available locations
            - If they asked for a specific year that doesn't exist, show available years
            - Always try to stay close to their original intent
            
            Return ONLY a valid JSON aggregation pipeline array.
            Include a $project stage that explains what alternative data is being shown.
            '''
            
            try:
                response = requests.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers=ai_svc.headers,
                    json={
                        "model": "anthropic/claude-3.5-sonnet",
                        "messages": [{"role": "user", "content": alternative_prompt}],
                        "temperature": 0.2
                    },
                    timeout=60
                )
                
                if response.status_code == 200:
                    result = response.json()
                    content = result['choices'][0]['message']['content'].strip()
                    
                    # Extract JSON array
                    start = content.find('[')
                    end = content.rfind(']') + 1
                    
                    if start != -1 and end > start:
                        json_str = content[start:end]
                        alternative_pipeline = json.loads(json_str)
                        
                        print(f"🔄 Alternative pipeline: {json.dumps(alternative_pipeline, indent=2)}")
                        
                        # Execute alternative pipeline
                        alternative_results = list(collection.aggregate(alternative_pipeline))
                        
                        if alternative_results:
                            return {
                                'results': alternative_results,
                                'pipeline': alternative_pipeline,
                                'explanation': f"Original query returned no results. Showing alternative analysis based on available data."
                            }
                            
            except Exception as e:
                print(f"❌ Claude alternative generation failed: {e}")
            
            print("🔧 Step 3: Using basic fallback alternatives...")
            
            # Basic fallback alternatives
            fallback_alternatives = [
                {
                    'name': 'Available months with data',
                    'pipeline': [
                        {"$group": {"_id": "$month", "count": {"$sum": 1}, "total_sales": {"$sum": {"$toDouble": "$Total"}}}},
                        {"$project": {"month": "$_id", "transactions": "$count", "total_sales": {"$round": ["$total_sales", 2]}, "_id": 0}},
                        {"$sort": {"month": 1}}
                    ]
                },
                {
                    'name': 'Sales by location (all available data)',
                    'pipeline': [
                        {"$group": {"_id": "$Location Name", "total_sales": {"$sum": {"$toDouble": "$Total"}}, "count": {"$sum": 1}}},
                        {"$project": {"location": "$_id", "total_sales": {"$round": ["$total_sales", 2]}, "transactions": "$count", "_id": 0}},
                        {"$sort": {"total_sales": -1}}
                    ]
                }
            ]
            
            for alternative in fallback_alternatives:
                try:
                    results = list(collection.aggregate(alternative['pipeline']))
                    if results:
                        print(f"✅ Using fallback: {alternative['name']}")
                        return {
                            'results': results,
                            'pipeline': alternative['pipeline'],
                            'explanation': f"No data found for original query. Showing: {alternative['name']}"
                        }
                except Exception as e:
                    print(f"❌ Fallback {alternative['name']} failed: {e}")
                    continue
            
            return None
            
        except Exception as e:
            print(f"❌ Error creating intelligent alternative: {e}")
            return None
    @ns_aggregate.doc('execute_aggregate')
    @ns_aggregate.expect(aggregate_request_model)
    @ns_aggregate.marshal_with(aggregate_response_model)
    @ns_aggregate.response(200, 'Success', aggregate_response_model)
    @ns_aggregate.response(400, 'Bad Request', error_model)
    @ns_aggregate.response(500, 'Internal Server Error', error_model)
    def post(self):
        """
        Execute MongoDB aggregation with AI assistance
        
        Flow:
        1. User posts command in Indonesian/English
        2. Mixtral translates to clear English
        3. Claude generates MongoDB aggregation pipeline
        4. Execute pipeline and return results
        """
        import time
        start_time = time.time()
        
        try:
            print("🚀 STEP 0: Starting aggregate execution")
            
            # Initialize services
            try:
                print("🔧 STEP 1: Initializing services...")
                ai_svc = get_ai_service()
                print("✅ AI service initialized")
                mongo_svc = get_mongo_connection()
                print("✅ MongoDB service initialized")
            except Exception as e:
                print(f"❌ Service initialization failed: {e}")
                return {
                    'success': False,
                    'error': 'Failed to initialize services',
                    'details': str(e)
                }, 500
            
            # Parse request
            print("📝 STEP 2: Parsing request...")
            data = request.get_json()
            if not data or 'command' not in data:
                print("❌ Invalid request data")
                return {
                    'success': False,
                    'error': 'Missing required field: command'
                }, 400
            
            user_command = data['command']
            collection_name = data.get('collection', 'transaction_sales')
            limit = data.get('limit', None)  # No default limit
            
            print(f"✅ Parsed: command='{user_command}', collection='{collection_name}', limit={limit}")
            logger.info(f"Processing command: {user_command}")
            
            # Step 1: Check for optimized collections
            print("🔍 STEP 3.5: Checking for optimized collections...")
            collection_builder = OptimizedCollectionBuilder()
            suggested_collection, confidence = collection_builder.suggest_collection_for_query(user_command)
            
            if suggested_collection and confidence >= 2:
                print(f"💡 Found optimized collection: {suggested_collection} (confidence: {confidence})")
                collection_name = suggested_collection
                print(f"🔄 Switching to optimized collection: {collection_name}")
            else:
                print(f"📊 Using original collection: {collection_name}")
            
            # Step 2: Connect to MongoDB
            print("🔌 STEP 3: Connecting to MongoDB...")
            client = mongo_svc.connect()
            if not client:
                print("❌ MongoDB connection failed")
                return {
                    'success': False,
                    'error': 'Failed to connect to MongoDB'
                }, 500
            print("✅ MongoDB connected successfully")
            
            # Step 2: Translate command with Mixtral
            print("🌐 STEP 4: Translating command with Mixtral...")
            logger.info("Translating command with Mixtral...")
            try:
                translated_command = ai_svc.translate_with_mixtral(user_command)
                print(f"✅ Translation successful: '{translated_command}'")
                
                # Simple translation validation and correction
                if "bulan juni" in user_command.lower() and "june" not in translated_command.lower():
                    print("🔧 Fixing translation: Adding June specification")
                    translated_command = translated_command.replace("per month", "for June").replace("in the year 2025", "for June 2024")
                
                if user_command.lower().count("per") == 1 and translated_command.lower().count("per") > 1:
                    print("🔧 Fixing translation: Removing extra 'per' specifications")
                    if "per lokasi" in user_command.lower() and "bulan juni" in user_command.lower():
                        translated_command = "Show sales by location for June"
                
                print(f"✅ Final translation: '{translated_command}'")
                logger.info(f"Translated command: {translated_command}")
            except Exception as e:
                print(f"❌ Translation failed: {e}")
                # Fallback translation
                if "per lokasi" in user_command.lower() and "juni" in user_command.lower():
                    translated_command = "Show sales by location for June"
                    print(f"🔧 Using fallback translation: '{translated_command}'")
                else:
                    raise
            
            # Step 3: Load schema and generate pipeline with Claude
            print("📋 STEP 5: Loading schema...")
            try:
                # Load appropriate schema based on collection being used
                if collection_name == 'transaction_sale':
                    schema_file = 'support/transaction_sale.json'
                else:
                    schema_file = f'support/{collection_name}.json'
                
                with open(schema_file, 'r') as f:
                    schema = json.load(f)
                print(f"✅ Schema loaded successfully for {collection_name}")
            except Exception as e:
                print(f"❌ Schema loading failed: {e}")
                # Fallback to transaction_sale schema
                try:
                    with open('support/transaction_sale.json', 'r') as f:
                        schema = json.load(f)
                    print("✅ Fallback to transaction_sale schema")
                except:
                    raise
            
            print("🤖 STEP 6: Generating pipeline with Claude...")
            logger.info("Generating pipeline with Claude...")
            try:
                pipeline = ai_svc.generate_pipeline_with_claude(translated_command, schema, collection_name)
                print(f"✅ Pipeline generated: {json.dumps(pipeline, indent=2)}")
                
                # Add limit only if specified and not already present
                has_limit = any('$limit' in str(stage) for stage in pipeline)
                if not has_limit and limit is not None:
                    pipeline.append({'$limit': limit})
                    print(f"➕ Added limit: {limit}")
                elif not has_limit and limit is None:
                    print("➡️ No limit specified - returning all results")
                
                logger.info(f"Generated pipeline: {pipeline}")
            except Exception as e:
                print(f"❌ Pipeline generation failed: {e}")
                print("🔄 Trying with predefined pipeline as fallback...")
                
                # Fallback to predefined pipeline based on command
                if "juni" in user_command.lower() or "june" in translated_command.lower():
                    # June sales by location
                    pipeline = [
                        {"$match": {"Sales Date": {"$regex": "^\\d{2}/06/\\d{4}$"}}},
                        {"$group": {"_id": "$Location Name", "total_sales": {"$sum": {"$toDouble": "$Total"}}, "count": {"$sum": 1}}},
                        {"$sort": {"total_sales": -1}}
                    ]
                    if limit is not None:
                        pipeline.append({"$limit": limit})
                    print(f"✅ Using June fallback pipeline: {json.dumps(pipeline, indent=2)}")
                elif "lokasi" in user_command.lower() or "location" in translated_command.lower():
                    # General sales by location
                    pipeline = [
                        {"$group": {"_id": "$Location Name", "total_sales": {"$sum": {"$toDouble": "$Total"}}, "count": {"$sum": 1}}},
                        {"$sort": {"total_sales": -1}}
                    ]
                    if limit is not None:
                        pipeline.append({"$limit": limit})
                    print(f"✅ Using location fallback pipeline: {json.dumps(pipeline, indent=2)}")
                else:
                    raise Exception(f"Claude pipeline generation failed and no suitable fallback: {e}")
            
            # Step 4: Check and update collection with month/year columns
            print("📅 STEP 7: Checking month/year columns...")
            try:
                db = mongo_svc.get_database()
                collection = db[collection_name]
                
                # Check collection exists and has data
                doc_count = collection.count_documents({})
                print(f"📊 Collection '{collection_name}' has {doc_count} documents")
                
                # Check if month/year columns exist
                sample_doc = collection.find_one({})
                if sample_doc and ('month' not in sample_doc or 'year' not in sample_doc):
                    print("🔄 Month/Year columns not found. Adding them...")
                    
                    # Update all documents to add month and year from Sales Date
                    update_pipeline = [
                        {
                            "$addFields": {
                                "month": {
                                    "$toInt": {
                                        "$substr": ["$Sales Date", 3, 2]
                                    }
                                },
                                "year": {
                                    "$toInt": {
                                        "$substr": ["$Sales Date", 6, 4]
                                    }
                                }
                            }
                        }
                    ]
                    
                    # Use aggregation pipeline to update documents
                    docs_to_update = list(collection.aggregate([
                        {"$match": {"$or": [{"month": {"$exists": False}}, {"year": {"$exists": False}}]}},
                        *update_pipeline
                    ]))
                    
                    if docs_to_update:
                        # Update documents in batches
                        for doc in docs_to_update:
                            collection.update_one(
                                {"_id": doc["_id"]},
                                {
                                    "$set": {
                                        "month": doc["month"],
                                        "year": doc["year"]
                                    }
                                }
                            )
                        
                        print(f"✅ Updated {len(docs_to_update)} documents with month/year columns")
                    else:
                        print("✅ All documents already have month/year columns")
                else:
                    print("✅ Month/Year columns already exist")
                    
            except Exception as e:
                print(f"⚠️  Warning: Could not update month/year columns: {e}")
                # Continue anyway - the aggregation might still work
            
            # Step 5: Execute aggregation
            print("⚡ STEP 8: Executing aggregation...")
            logger.info("Executing aggregation...")
            try:
                
                results = list(collection.aggregate(pipeline))
                
                # Convert ObjectId to string for JSON serialization
                for result in results:
                    if '_id' in result and hasattr(result['_id'], '__class__') and result['_id'].__class__.__name__ == 'ObjectId':
                        result['_id'] = str(result['_id'])
                print(f"✅ Aggregation executed successfully: {len(results)} results")
                
                # If no results found, try to create intelligent alternatives
                if len(results) == 0:
                    print("🔍 No results found. Analyzing existing data for alternatives...")
                    alternative_results = self.create_intelligent_alternative(collection, user_command, translated_command, ai_svc, schema)
                    if alternative_results:
                        results = alternative_results['results']
                        pipeline = alternative_results['pipeline']
                        print(f"✅ Found alternative solution: {len(results)} results")
                
                # Print detailed aggregation results
                print("\n" + "="*60)
                print("📊 AGGREGATION RESULTS:")
                print("="*60)
                formatted_results = []
                for i, result in enumerate(results, 1):
                    formatted_result = f"{i:2d}. {json.dumps(result, indent=4, default=str)}"
                    print(formatted_result)
                    formatted_results.append(formatted_result)
                print("="*60)
                
                execution_time = time.time() - start_time
                print(f"⏱️ Total execution time: {execution_time:.2f}s")
                
                logger.info(f"Aggregation completed in {execution_time:.2f}s with {len(results)} results")
                
                # Step 6: Generate analytics description with Mixtral
                print("📊 STEP 9: Generating analytics description with Mixtral...")
                analytics_description = ai_svc.analyze_results_with_mixtral(user_command, results, pipeline)
                print(f"✅ Analytics description generated: {len(analytics_description)} characters")
                print(f"📝 Preview: {analytics_description[:200]}...")
            except Exception as e:
                print(f"❌ Aggregation execution failed: {e}")
                raise
            
            # Prepare response with MongoDB query results and analytics
            response_data = {
                'success': True,
                'original_command': user_command,
                'translated_command': translated_command,
                'generated_pipeline': pipeline,
                'results': results,  # The actual MongoDB query results
                'description': analytics_description,  # Detailed analytics description by Mixtral
                'total_results': len(results),
                'execution_time': execution_time,
                'collection_used': collection_name,
                'documents_in_collection': doc_count,
                'alternative_used': hasattr(alternative_results, 'explanation') if 'alternative_results' in locals() else False,
                'explanation': alternative_results.get('explanation') if 'alternative_results' in locals() and alternative_results else None
            }
            
            print(f"📤 RESPONSE PREPARED: {len(results)} MongoDB results ready")
            return response_data
            
        except json.JSONDecodeError as e:
            return {
                'success': False,
                'error': 'Invalid JSON in generated pipeline',
                'details': str(e)
            }, 400
            
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            logger.error(f"Error executing aggregate: {e}")
            logger.error(f"Full traceback: {error_details}")
            return {
                'success': False,
                'error': 'Internal server error',
                'details': str(e),
                'traceback': error_details
            }, 500
            
        finally:
            if 'mongo_svc' in locals():
                mongo_svc.disconnect()

@ns_aggregate.route('/pipelines')
class ListPipelines(Resource):
    @ns_aggregate.doc('list_predefined_pipelines')
    def get(self):
        """Get list of predefined aggregation pipelines"""
        try:
            mongo_svc = get_mongo_connection()
            pipelines = mongo_svc.create_aggregation_pipeline()
            return {
                'success': True,
                'available_pipelines': list(pipelines.keys()),
                'pipeline_descriptions': {
                    'sales_by_location': 'Sales summary grouped by location',
                    'daily_sales': 'Daily sales trend over time',
                    'product_performance': 'Product performance analysis',
                    'payment_methods': 'Payment method analysis',
                    'hourly_pattern': 'Hourly sales pattern analysis'
                }
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}, 500

@ns_aggregate.route('/collections')
class CollectionManager(Resource):
    @ns_aggregate.doc('list_optimized_collections')
    def get(self):
        """List all available optimized collections"""
        try:
            collection_builder = OptimizedCollectionBuilder()
            if not collection_builder.connect_db():
                return {'error': 'Cannot connect to database'}, 500
            
            # Get all collection info
            collections_info = {}
            for collection_name, schema in collection_builder.collection_schemas.items():
                try:
                    collection = collection_builder.db[collection_name]
                    doc_count = collection.count_documents({})
                    last_record = collection.find_one(sort=[("last_updated", -1)])
                    last_updated = last_record.get('last_updated') if last_record else None
                    
                    collections_info[collection_name] = {
                        'description': schema.get('description'),
                        'purpose': schema.get('purpose'),
                        'document_count': doc_count,
                        'last_updated': str(last_updated) if last_updated else None,
                        'sample_queries': schema.get('sample_queries', [])
                    }
                except Exception as e:
                    collections_info[collection_name] = {
                        'description': schema.get('description'),
                        'purpose': schema.get('purpose'),
                        'document_count': 0,
                        'last_updated': None,
                        'sample_queries': schema.get('sample_queries', []),
                        'error': f'Collection not built: {str(e)}'
                    }
            
            collection_builder.mongo_conn.disconnect()
            
            return {
                'success': True,
                'optimized_collections': collections_info,
                'total_collections': len(collections_info)
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}, 500
    
    @ns_aggregate.doc('rebuild_collections')
    def post(self):
        """Rebuild all optimized collections"""
        try:
            collection_builder = OptimizedCollectionBuilder()
            success = collection_builder.build_all_collections()
            
            return {
                'success': success,
                'message': 'All collections rebuilt successfully' if success else 'Some collections failed to rebuild'
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}, 500

@ns_aggregate.route('/collections/<string:collection_name>')
class SingleCollectionManager(Resource):
    @ns_aggregate.doc('rebuild_single_collection')
    def post(self, collection_name):
        """Rebuild a specific optimized collection"""
        try:
            collection_builder = OptimizedCollectionBuilder()
            success = collection_builder.build_single_collection(collection_name)
            
            if success:
                return {
                    'success': True,
                    'message': f'Collection {collection_name} rebuilt successfully'
                }
            else:
                return {
                    'success': False,
                    'error': f'Failed to rebuild collection {collection_name}'
                }, 400
                
        except Exception as e:
            return {'success': False, 'error': str(e)}, 500

@ns_aggregate.route('/pipelines/<string:pipeline_name>')
class ExecutePredefinedPipeline(Resource):
    @ns_aggregate.doc('execute_predefined_pipeline')
    def post(self, pipeline_name):
        """Execute a predefined aggregation pipeline"""
        try:
            mongo_svc = get_mongo_connection()
            
            # Connect and execute
            client = mongo_svc.connect()
            if not client:
                return {'error': 'Failed to connect to MongoDB'}, 500
            
            results = mongo_svc.run_aggregation(pipeline_name)
            
            return {
                'success': True,
                'pipeline_name': pipeline_name,
                'results': results,
                'total_results': len(results)
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}, 400
        finally:
            if 'mongo_svc' in locals():
                mongo_svc.disconnect()

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return {
        'status': 'healthy',
        'services': {
            'ai_service': bool(os.getenv('OPENROUTER_API_KEY')),
            'mongodb': True,  # Will be checked on connection
            'openrouter_key': bool(os.getenv('OPENROUTER_API_KEY'))
        }
    }

if __name__ == '__main__':
    port = 5002  # Changed to avoid conflict
    print("🚀 Starting Transaction Sale Aggregate API")
    print(f"📚 Swagger docs available at: http://localhost:{port}/docs")
    print(f"🏥 Health check at: http://localhost:{port}/health")
    
    app.run(debug=True, host='0.0.0.0', port=port)