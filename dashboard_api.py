#!/usr/bin/env python3
"""
Additional API endpoints for dashboard
"""

# Load environment variables first
import load_env

from flask import Flask, request, jsonify
from flask_restx import Api, Resource, fields, Namespace
from mongodb_connection import MongoDBSSHConnection
from collection_builder import OptimizedCollectionBuilder
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
api = Api(
    app, 
    version='1.0', 
    title='Dashboard API',
    description='API endpoints for analytics dashboard',
    doc='/dashboard-docs'
)

# Namespaces
ns_dashboard = Namespace('dashboard', description='Dashboard Data Operations')
api.add_namespace(ns_dashboard)

def get_mongo_connection():
    """Get MongoDB connection"""
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    if client:
        db = mongo_conn.get_database()
        return mongo_conn, db
    return None, None

@ns_dashboard.route('/total-sales')
class TotalSales(Resource):
    def get(self):
        """Get total sales across all locations"""
        try:
            mongo_conn, db = get_mongo_connection()
            if db is None:
                return {'error': 'Database connection failed'}, 500
            
            collection = db['sales_by_location']
            pipeline = [
                {
                    "$group": {
                        "_id": None,
                        "total_sales": {"$sum": "$total_sales"},
                        "total_transactions": {"$sum": "$total_transactions"}
                    }
                }
            ]
            
            result = list(collection.aggregate(pipeline))
            mongo_conn.disconnect()
            
            if result:
                return {
                    'success': True,
                    'data': result[0]
                }
            return {'success': False, 'data': {'total_sales': 0, 'total_transactions': 0}}
            
        except Exception as e:
            logger.error(f"Error getting total sales: {e}")
            return {'error': str(e)}, 500

@ns_dashboard.route('/locations')
class Locations(Resource):
    def get(self):
        """Get all locations"""
        try:
            mongo_conn, db = get_mongo_connection()
            if db is None:
                return {'error': 'Database connection failed'}, 500
            
            collection = db['sales_by_location']
            pipeline = [
                {
                    "$project": {
                        "location_name": 1,
                        "total_sales": 1,
                        "total_transactions": 1,
                        "_id": 0
                    }
                },
                {"$sort": {"total_sales": -1}}
            ]
            
            result = list(collection.aggregate(pipeline))
            mongo_conn.disconnect()
            
            return {
                'success': True,
                'data': result
            }
            
        except Exception as e:
            logger.error(f"Error getting locations: {e}")
            return {'error': str(e)}, 500

@ns_dashboard.route('/product-categories')
class ProductCategories(Resource):
    def get(self):
        """Get all product categories"""
        try:
            mongo_conn, db = get_mongo_connection()
            if db is None:
                return {'error': 'Database connection failed'}, 500
            
            collection = db['sales_by_product']
            pipeline = [
                {
                    "$group": {
                        "_id": "$product_category",
                        "total_revenue": {"$sum": "$total_revenue"},
                        "total_quantity": {"$sum": "$total_quantity_sold"}
                    }
                },
                {
                    "$project": {
                        "product_category": "$_id",
                        "total_revenue": 1,
                        "total_quantity": 1,
                        "_id": 0
                    }
                },
                {"$sort": {"total_revenue": -1}}
            ]
            
            result = list(collection.aggregate(pipeline))
            mongo_conn.disconnect()
            
            return {
                'success': True,
                'data': result
            }
            
        except Exception as e:
            logger.error(f"Error getting product categories: {e}")
            return {'error': str(e)}, 500

@ns_dashboard.route('/monthly-sales')
class MonthlySales(Resource):
    def get(self):
        """Get monthly sales data"""
        try:
            mongo_conn, db = get_mongo_connection()
            if db is None:
                return {'error': 'Database connection failed'}, 500
            
            collection = db['sales_by_month']
            pipeline = [
                {
                    "$project": {
                        "month": 1,
                        "month_name": 1,
                        "year": 1,
                        "total_sales": 1,
                        "total_transactions": 1,
                        "_id": 0
                    }
                },
                {"$sort": {"year": 1, "month": 1}}
            ]
            
            result = list(collection.aggregate(pipeline))
            mongo_conn.disconnect()
            
            return {
                'success': True,
                'data': result
            }
            
        except Exception as e:
            logger.error(f"Error getting monthly sales: {e}")
            return {'error': str(e)}, 500

@ns_dashboard.route('/payment-methods')
class PaymentMethods(Resource):
    def get(self):
        """Get payment method distribution"""
        try:
            mongo_conn, db = get_mongo_connection()
            if db is None:
                return {'error': 'Database connection failed'}, 500
            
            collection = db['sales_by_payment_method']
            pipeline = [
                {
                    "$project": {
                        "payment_method": 1,
                        "total_sales": 1,
                        "total_transactions": 1,
                        "_id": 0
                    }
                },
                {"$sort": {"total_sales": -1}}
            ]
            
            result = list(collection.aggregate(pipeline))
            mongo_conn.disconnect()
            
            return {
                'success': True,
                'data': result
            }
            
        except Exception as e:
            logger.error(f"Error getting payment methods: {e}")
            return {'error': str(e)}, 500

@ns_dashboard.route('/top-products')
class TopProducts(Resource):
    def get(self):
        """Get top products"""
        try:
            limit = request.args.get('limit', 20, type=int)
            
            mongo_conn, db = get_mongo_connection()
            if db is None:
                return {'error': 'Database connection failed'}, 500
            
            collection = db['sales_by_product']
            pipeline = [
                {
                    "$project": {
                        "product_name": 1,
                        "product_category": 1,
                        "total_revenue": 1,
                        "total_quantity_sold": 1,
                        "_id": 0
                    }
                },
                {"$sort": {"total_revenue": -1}},
                {"$limit": limit}
            ]
            
            result = list(collection.aggregate(pipeline))
            mongo_conn.disconnect()
            
            return {
                'success': True,
                'data': result
            }
            
        except Exception as e:
            logger.error(f"Error getting top products: {e}")
            return {'error': str(e)}, 500

@ns_dashboard.route('/kpi-summary')
class KPISummary(Resource):
    def get(self):
        """Get KPI summary data"""
        try:
            mongo_conn, db = get_mongo_connection()
            if db is None:
                return {'error': 'Database connection failed'}, 500
            
            # Get total sales from locations
            locations_collection = db['sales_by_location']
            sales_result = list(locations_collection.aggregate([
                {"$group": {
                    "_id": None,
                    "total_sales": {"$sum": "$total_sales"},
                    "total_transactions": {"$sum": "$total_transactions"},
                    "total_locations": {"$sum": 1}
                }}
            ]))
            
            # Get average transaction
            avg_transaction = 0
            if sales_result and sales_result[0]['total_transactions'] > 0:
                avg_transaction = sales_result[0]['total_sales'] / sales_result[0]['total_transactions']
            
            # Get product count
            products_collection = db['sales_by_product']
            product_count = products_collection.count_documents({})
            
            mongo_conn.disconnect()
            
            kpi_data = {
                'total_sales': sales_result[0]['total_sales'] if sales_result else 0,
                'total_transactions': sales_result[0]['total_transactions'] if sales_result else 0,
                'total_locations': sales_result[0]['total_locations'] if sales_result else 0,
                'average_transaction': avg_transaction,
                'total_products': product_count
            }
            
            return {
                'success': True,
                'data': kpi_data
            }
            
        except Exception as e:
            logger.error(f"Error getting KPI summary: {e}")
            return {'error': str(e)}, 500

@app.route('/dashboard-health')
def dashboard_health():
    """Dashboard API health check"""
    try:
        mongo_conn, db = get_mongo_connection()
        if db is not None:
            collections = db.list_collection_names()
            mongo_conn.disconnect()
            return {
                'status': 'healthy',
                'collections': len(collections),
                'available_collections': collections
            }
        else:
            return {'status': 'unhealthy', 'error': 'Database connection failed'}, 500
    except Exception as e:
        return {'status': 'unhealthy', 'error': str(e)}, 500

if __name__ == '__main__':
    port = 5003  # Different port for dashboard API
    print("🚀 Starting Dashboard API")
    print(f"📚 Dashboard docs available at: http://localhost:{port}/dashboard-docs")
    print(f"🏥 Health check at: http://localhost:{port}/dashboard-health")
    
    app.run(debug=True, host='0.0.0.0', port=port)