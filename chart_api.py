#!/usr/bin/env python3
"""
Individual Chart API Endpoints
Each chart has its own endpoint with specific filters
"""

# Load environment variables first
import load_env

from flask import Flask, request, jsonify
from flask_restx import Api, Resource, fields, Namespace
from mongodb_connection import MongoDBSSHConnection
from collection_builder import OptimizedCollectionBuilder
import json
import logging
import random
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
api = Api(
    app, 
    version='1.0', 
    title='Chart API',
    description='Individual API endpoints for each chart with filters',
    doc='/chart-docs'
)

# Namespaces
ns_chart = Namespace('chart', description='Individual Chart Data Operations')
api.add_namespace(ns_chart)

def get_mongo_connection():
    """Get MongoDB connection"""
    mongo_conn = MongoDBSSHConnection()
    client = mongo_conn.connect()
    if client:
        db = mongo_conn.get_database()
        return mongo_conn, db
    return None, None

@ns_chart.route('/sales-trend')
class SalesTrendChart(Resource):
    def get(self):
        """Sales Trend Line Chart with Date Range and Interval Support"""
        try:
            # Get date range parameters or fall back to year
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            year = request.args.get('year', 2025, type=int)
            interval = request.args.get('interval', 'monthly')  # monthly, weekly, daily
            locations = request.args.getlist('locations')  # Support multiple locations
            
            mongo_conn, db = get_mongo_connection()
            if db is None:
                return {'error': 'Database connection failed'}, 500
            
            # Build date and location filters
            date_filter = {}
            location_filter = {}
            
            # Add location filter if specified
            if locations:
                location_filter = {"location_name": {"$in": locations}}
            if start_date and end_date:
                from datetime import datetime
                start_dt = datetime.fromisoformat(start_date)
                end_dt = datetime.fromisoformat(end_date)
                
                if interval.lower() == 'monthly':
                    # For monthly data, filter by year and month
                    date_filter = {
                        "$and": [
                            {
                                "$or": [
                                    {"year": {"$gt": start_dt.year}},
                                    {
                                        "$and": [
                                            {"year": start_dt.year},
                                            {"month": {"$gte": start_dt.month}}
                                        ]
                                    }
                                ]
                            },
                            {
                                "$or": [
                                    {"year": {"$lt": end_dt.year}},
                                    {
                                        "$and": [
                                            {"year": end_dt.year},
                                            {"month": {"$lte": end_dt.month}}
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                else:
                    # For weekly/daily data, filter by date field
                    if interval.lower() == 'weekly':
                        date_filter = {
                            "$or": [
                                {"start_date": {"$gte": start_dt, "$lte": end_dt}},
                                {"end_date": {"$gte": start_dt, "$lte": end_dt}},
                                {"$and": [
                                    {"start_date": {"$lte": start_dt}},
                                    {"end_date": {"$gte": end_dt}}
                                ]}
                            ]
                        }
                    else:  # daily
                        date_filter = {
                            "date": {
                                "$gte": start_dt,
                                "$lte": end_dt
                            }
                        }
            else:
                # Fallback to year filter
                date_filter = {"year": year}
            
            if interval.lower() == 'monthly':
                if locations:
                    # Use location-based monthly data
                    collection = db['sales_by_location_day']
                    
                    # Combine filters
                    match_filter = {**date_filter, **location_filter}
                    if not date_filter:  # If no date filter, use year
                        match_filter = {**location_filter, "year": year}
                    
                    pipeline = [
                        {"$match": match_filter},
                        {
                            "$group": {
                                "_id": {"year": "$year", "month": "$month"},
                                "total_sales": {"$sum": "$total_sales"},
                                "total_transactions": {"$sum": "$total_transactions"}
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
                        {"$sort": {"_id.year": 1, "_id.month": 1}}
                    ]
                    
                    result = list(collection.aggregate(pipeline))
                    
                    # Format for line chart
                    chart_data = {
                        'x': [item.get('month_name', f"Month {item.get('_id', {}).get('month', '')}") for item in result],
                        'y': [item.get('total_sales', 0) for item in result],
                        'type': 'line',
                        'name': f'Sales Trend - {len(locations)} Location(s)',
                        'mode': 'lines+markers',
                        'fill': 'tonexty'
                    }
                else:
                    # Use aggregated monthly data for all locations
                    collection = db['sales_by_month']
                    pipeline = [
                        {"$match": date_filter if date_filter else {"year": year}},
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
                    
                    # Format for line chart
                    chart_data = {
                        'x': [item.get('month_name', f"Month {item.get('month', '')}") for item in result],
                        'y': [item.get('total_sales', 0) for item in result],
                        'type': 'line',
                        'name': 'Sales Trend (All Locations)',
                        'mode': 'lines+markers',
                        'fill': 'tonexty'
                    }
                
            elif interval.lower() == 'weekly':
                if locations:
                    # Use location-based weekly data
                    collection = db['sales_by_location_week']
                    
                    # Combine filters
                    match_filter = {**date_filter, **location_filter}
                    if not date_filter:  # If no date filter, use year
                        match_filter = {**location_filter, "year": year}
                    
                    pipeline = [
                        {"$match": match_filter},
                        {
                            "$group": {
                                "_id": {"year": "$year", "iso_week": "$iso_week"},
                                "total_sales": {"$sum": "$total_sales"},
                                "total_transactions": {"$sum": "$total_transactions"},
                                "week_label": {"$first": "$week_label"}
                            }
                        },
                        {"$sort": {"_id.year": 1, "_id.iso_week": 1}}
                    ]
                else:
                    # Use aggregated weekly data for all locations
                    collection = db['sales_by_week']
                    pipeline = [
                        {"$match": date_filter if date_filter else {"year": year}},
                        {
                            "$project": {
                                "week_label": 1,
                                "iso_week": 1,
                                "total_sales": 1,
                                "total_transactions": 1,
                                "start_date": 1,
                                "_id": 0
                            }
                        },
                    {"$sort": {"start_date": 1}}
                ]
                
                result = list(collection.aggregate(pipeline))
                logger.info(f"Weekly sales trend query returned {len(result)} documents")
                
                # Format for line chart
                chart_data = {
                    'x': [item.get('week_label', f"Week {item.get('iso_week', '')}") for item in result],
                    'y': [item.get('total_sales', 0) for item in result],
                    'type': 'line',
                    'name': 'Sales Trend (Weekly)',
                    'mode': 'lines+markers',
                    'fill': 'tonexty'
                }
                
            elif interval.lower() == 'daily':
                # Use daily data
                collection = db['sales_by_day']
                pipeline = [
                    {"$match": date_filter},
                    {
                        "$project": {
                            "display_date": 1,
                            "date": 1,
                            "total_sales": 1,
                            "total_transactions": 1,
                            "_id": 0
                        }
                    },
                    {"$sort": {"date": 1}},
                    {"$limit": 60}  # Limit to 60 days
                ]
                
                result = list(collection.aggregate(pipeline))
                logger.info(f"Daily sales trend query returned {len(result)} documents")
                
                # Format for line chart
                chart_data = {
                    'x': [item.get('display_date', f"Day {i+1}") for i, item in enumerate(result)],
                    'y': [item.get('total_sales', 0) for item in result],
                    'type': 'line',
                    'name': 'Sales Trend (Daily)',
                    'mode': 'lines+markers',
                    'fill': 'tonexty'
                }
            
            else:
                chart_data = {
                    'x': [],
                    'y': [],
                    'type': 'line',
                    'name': 'Sales Trend',
                    'mode': 'lines+markers'
                }
            
            mongo_conn.disconnect()
            
            # Create title based on date range or year
            if start_date and end_date:
                title = f'Sales Trend - {start_date} to {end_date} ({interval.title()})'
            else:
                title = f'Sales Trend - {year} ({interval.title()})'
            
            return {
                'success': True,
                'chart_type': 'line',
                'data': chart_data,
                'title': title,
                'start_date': start_date,
                'end_date': end_date,
                'year': year,
                'interval': interval
            }
            
        except Exception as e:
            logger.error(f"Error getting sales trend: {e}")
            return {'error': str(e)}, 500

@ns_chart.route('/location-performance')
class LocationPerformanceChart(Resource):
    def get(self):
        """Location Performance Line Chart with Date Range and Interval Support"""
        try:
            # Get date range parameters or fall back to year
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            year = request.args.get('year', 2025, type=int)
            interval = request.args.get('interval', 'monthly')  # monthly, weekly, daily
            locations = request.args.getlist('locations')
            limit = request.args.get('limit', 5, type=int)  # Reduced default for better visualization
            
            mongo_conn, db = get_mongo_connection()
            if db is None:
                return {'error': 'Database connection failed'}, 500
            
            # Build date filter
            date_filter = {}
            if start_date and end_date:
                from datetime import datetime
                start_dt = datetime.fromisoformat(start_date)
                end_dt = datetime.fromisoformat(end_date)
                
                if interval.lower() == 'monthly':
                    # For monthly data, filter by year and month
                    date_filter = {
                        "$and": [
                            {
                                "$or": [
                                    {"year": {"$gt": start_dt.year}},
                                    {
                                        "$and": [
                                            {"year": start_dt.year},
                                            {"month": {"$gte": start_dt.month}}
                                        ]
                                    }
                                ]
                            },
                            {
                                "$or": [
                                    {"year": {"$lt": end_dt.year}},
                                    {
                                        "$and": [
                                            {"year": end_dt.year},
                                            {"month": {"$lte": end_dt.month}}
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                else:
                    # For weekly/daily data, filter by date field
                    if interval.lower() == 'weekly':
                        date_filter = {
                            "$or": [
                                {"start_date": {"$gte": start_dt, "$lte": end_dt}},
                                {"end_date": {"$gte": start_dt, "$lte": end_dt}},
                                {"$and": [
                                    {"start_date": {"$lte": start_dt}},
                                    {"end_date": {"$gte": end_dt}}
                                ]}
                            ]
                        }
                    else:  # daily
                        date_filter = {
                            "date": {
                                "$gte": start_dt,
                                "$lte": end_dt
                            }
                        }
            else:
                # Fallback to year filter
                date_filter = {"year": year}
            
            if interval.lower() == 'monthly':
                # Use monthly location data
                collection = db['sales_by_location_month']
                
                # First get top locations by total sales for the period
                location_pipeline = [
                    {"$match": date_filter},
                    {
                        "$group": {
                            "_id": "$location_name",
                            "total_sales": {"$sum": "$total_sales"}
                        }
                    },
                    {"$sort": {"total_sales": -1}},
                    {"$limit": limit}
                ]
                
                top_locations = list(collection.aggregate(location_pipeline))
                top_location_names = [loc["_id"] for loc in top_locations]
                
                # Now get monthly trend data for these top locations
                pipeline = [
                    {
                        "$match": {
                            **date_filter,
                            "location_name": {"$in": top_location_names}
                        }
                    },
                    {
                        "$project": {
                            "month": 1,
                            "month_name": 1,
                            "year": 1,
                            "location_name": 1,
                            "total_sales": 1,
                            "total_transactions": 1,
                            "_id": 0
                        }
                    },
                    {"$sort": {"year": 1, "month": 1, "location_name": 1}}
                ]
                
                result = list(collection.aggregate(pipeline))
                
                # Group by location and create multiple line series
                location_data = {}
                time_labels = set()
                
                for item in result:
                    location = item.get('location_name', '')
                    time_label = item.get('month_name', f"Month {item.get('month', '')}")
                    sales = item.get('total_sales', 0)
                    
                    if location not in location_data:
                        location_data[location] = {}
                    location_data[location][time_label] = sales
                    time_labels.add(time_label)
                
                # Sort time labels chronologically
                month_order = ["January", "February", "March", "April", "May", "June",
                              "July", "August", "September", "October", "November", "December"]
                time_labels = sorted(time_labels, key=lambda x: month_order.index(x) if x in month_order else 999)
                
                # Create chart data with multiple series
                chart_data = []
                for location, data in location_data.items():
                    y_values = [data.get(label, 0) for label in time_labels]
                    chart_data.append({
                        'x': time_labels,
                        'y': y_values,
                        'type': 'line',
                        'name': f'{location}',
                        'mode': 'lines+markers'
                    })
                
            elif interval.lower() == 'weekly':
                # Use weekly location data
                collection = db['sales_by_location_week']
                
                # First get top locations by total sales for the period
                location_pipeline = [
                    {"$match": date_filter},
                    {
                        "$group": {
                            "_id": "$location_name",
                            "total_sales": {"$sum": "$total_sales"}
                        }
                    },
                    {"$sort": {"total_sales": -1}},
                    {"$limit": limit}
                ]
                
                top_locations = list(collection.aggregate(location_pipeline))
                top_location_names = [loc["_id"] for loc in top_locations]
                
                # Now get weekly trend data for these top locations
                pipeline = [
                    {
                        "$match": {
                            **date_filter,
                            "location_name": {"$in": top_location_names}
                        }
                    },
                    {
                        "$project": {
                            "week_label": 1,
                            "iso_week": 1,
                            "location_name": 1,
                            "total_sales": 1,
                            "total_transactions": 1,
                            "start_date": 1,
                            "_id": 0
                        }
                    },
                    {"$sort": {"start_date": 1, "location_name": 1}}
                ]
                
                result = list(collection.aggregate(pipeline))
                logger.info(f"Weekly location performance query returned {len(result)} documents")
                
                # Group by location and create multiple line series
                location_data = {}
                time_labels = set()
                
                for item in result:
                    location = item.get('location_name', '')
                    time_label = item.get('week_label', f"Week {item.get('iso_week', '')}")
                    sales = item.get('total_sales', 0)
                    
                    if location not in location_data:
                        location_data[location] = {}
                    location_data[location][time_label] = sales
                    time_labels.add(time_label)
                
                # Sort time labels chronologically
                time_labels = sorted(time_labels)
                
                # Create chart data with multiple series
                chart_data = []
                for location, data in location_data.items():
                    y_values = [data.get(label, 0) for label in time_labels]
                    chart_data.append({
                        'x': time_labels,
                        'y': y_values,
                        'type': 'line',
                        'name': f'{location}',
                        'mode': 'lines+markers'
                    })
                
            elif interval.lower() == 'daily':
                # Use daily location data
                collection = db['sales_by_location_day']
                
                # First get top locations by total sales for the period
                location_pipeline = [
                    {"$match": date_filter},
                    {
                        "$group": {
                            "_id": "$location_name",
                            "total_sales": {"$sum": "$total_sales"}
                        }
                    },
                    {"$sort": {"total_sales": -1}},
                    {"$limit": limit}
                ]
                
                top_locations = list(collection.aggregate(location_pipeline))
                top_location_names = [loc["_id"] for loc in top_locations]
                
                # Now get daily trend data for these top locations
                pipeline = [
                    {
                        "$match": {
                            **date_filter,
                            "location_name": {"$in": top_location_names}
                        }
                    },
                    {
                        "$project": {
                            "display_date": 1,
                            "date": 1,
                            "location_name": 1,
                            "total_sales": 1,
                            "total_transactions": 1,
                            "_id": 0
                        }
                    },
                    {"$sort": {"date": 1, "location_name": 1}},
                    {"$limit": 300}  # Limit to 300 days for performance
                ]
                
                result = list(collection.aggregate(pipeline))
                logger.info(f"Daily location performance query returned {len(result)} documents")
                
                # Group by location and create multiple line series
                location_data = {}
                time_labels = set()
                
                for item in result:
                    location = item.get('location_name', '')
                    time_label = item.get('display_date', '')
                    sales = item.get('total_sales', 0)
                    
                    if location not in location_data:
                        location_data[location] = {}
                    location_data[location][time_label] = sales
                    time_labels.add(time_label)
                
                # Sort time labels chronologically
                time_labels = sorted(time_labels)
                
                # Create chart data with multiple series
                chart_data = []
                for location, data in location_data.items():
                    y_values = [data.get(label, 0) for label in time_labels]
                    chart_data.append({
                        'x': time_labels,
                        'y': y_values,
                        'type': 'line',
                        'name': f'{location}',
                        'mode': 'lines+markers'
                    })
            
            else:
                chart_data = []
            
            # Add location filter if specified after getting top locations
            if locations and locations != ['all']:
                # Filter chart_data to only include specified locations
                chart_data = [series for series in chart_data if series['name'] in locations]
            
            mongo_conn.disconnect()
            
            # Create title based on date range or year
            if start_date and end_date:
                title = f'Location Performance Trends - {start_date} to {end_date} ({interval.title()})'
            else:
                title = f'Location Performance Trends - {year} ({interval.title()})'
            
            return {
                'success': True,
                'chart_type': 'line',
                'data': chart_data,
                'title': title,
                'start_date': start_date,
                'end_date': end_date,
                'year': year,
                'interval': interval,
                'top_locations': len(chart_data)
            }
            
        except Exception as e:
            logger.error(f"Error getting location performance: {e}")
            return {'error': str(e)}, 500

@ns_chart.route('/product-trend')
class ProductTrendChart(Resource):
    def get(self):
        """Product Category Trend Line Chart with Date Range and Interval Support"""
        try:
            # Get date range parameters or fall back to year
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            year = request.args.get('year', 2025, type=int)
            interval = request.args.get('interval', 'monthly')  # monthly, weekly, daily
            categories = request.args.getlist('categories')
            limit = request.args.get('limit', 5, type=int)  # Reduced default for better visualization
            
            mongo_conn, db = get_mongo_connection()
            if db is None:
                return {'error': 'Database connection failed'}, 500
            
            # Build date filter
            date_filter = {}
            if start_date and end_date:
                from datetime import datetime
                start_dt = datetime.fromisoformat(start_date)
                end_dt = datetime.fromisoformat(end_date)
                
                if interval.lower() == 'monthly':
                    # For monthly data, filter by year and month
                    date_filter = {
                        "$and": [
                            {
                                "$or": [
                                    {"year": {"$gt": start_dt.year}},
                                    {
                                        "$and": [
                                            {"year": start_dt.year},
                                            {"month": {"$gte": start_dt.month}}
                                        ]
                                    }
                                ]
                            },
                            {
                                "$or": [
                                    {"year": {"$lt": end_dt.year}},
                                    {
                                        "$and": [
                                            {"year": end_dt.year},
                                            {"month": {"$lte": end_dt.month}}
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                else:
                    # For weekly/daily data, filter by date field
                    if interval.lower() == 'weekly':
                        date_filter = {
                            "$or": [
                                {"start_date": {"$gte": start_dt, "$lte": end_dt}},
                                {"end_date": {"$gte": start_dt, "$lte": end_dt}},
                                {"$and": [
                                    {"start_date": {"$lte": start_dt}},
                                    {"end_date": {"$gte": end_dt}}
                                ]}
                            ]
                        }
                    else:  # daily
                        date_filter = {
                            "date": {
                                "$gte": start_dt,
                                "$lte": end_dt
                            }
                        }
            else:
                # Fallback to year filter
                date_filter = {"year": year}
            
            if interval.lower() == 'monthly':
                # Use monthly product data
                collection = db['sales_by_product_month']
                
                # First get top products by total revenue for the period
                product_pipeline = [
                    {"$match": date_filter},
                    {
                        "$group": {
                            "_id": "$product_name",
                            "total_revenue": {"$sum": "$total_revenue"}
                        }
                    },
                    {"$sort": {"total_revenue": -1}},
                    {"$limit": limit}
                ]
                
                top_products = list(collection.aggregate(product_pipeline))
                top_product_names = [prod["_id"] for prod in top_products]
                
                # Now get monthly trend data for these top products
                pipeline = [
                    {
                        "$match": {
                            **date_filter,
                            "product_name": {"$in": top_product_names}
                        }
                    },
                    {
                        "$project": {
                            "month": 1,
                            "month_name": 1,
                            "year": 1,
                            "product_name": 1,
                            "total_revenue": 1,
                            "total_quantity_sold": 1,
                            "total_transactions": 1,
                            "_id": 0
                        }
                    },
                    {"$sort": {"year": 1, "month": 1, "product_name": 1}}
                ]
                
                result = list(collection.aggregate(pipeline))
                
                # Group by product and create multiple line series
                product_data = {}
                time_labels = set()
                
                for item in result:
                    product = item.get('product_name', '')
                    time_label = item.get('month_name', f"Month {item.get('month', '')}")
                    revenue = item.get('total_revenue', 0)
                    
                    if product not in product_data:
                        product_data[product] = {}
                    product_data[product][time_label] = revenue
                    time_labels.add(time_label)
                
                # Sort time labels chronologically
                month_order = ["January", "February", "March", "April", "May", "June",
                              "July", "August", "September", "October", "November", "December"]
                time_labels = sorted(time_labels, key=lambda x: month_order.index(x) if x in month_order else 999)
                
                # Create chart data with multiple series
                chart_data = []
                for product, data in product_data.items():
                    y_values = [data.get(label, 0) for label in time_labels]
                    chart_data.append({
                        'x': time_labels,
                        'y': y_values,
                        'type': 'line',
                        'name': f'{product}',
                        'mode': 'lines+markers'
                    })
                
            elif interval.lower() == 'weekly':
                # Use weekly product data
                collection = db['sales_by_product_week']
                
                # First get top products by total revenue for the period
                product_pipeline = [
                    {"$match": date_filter},
                    {
                        "$group": {
                            "_id": "$product_name",
                            "total_revenue": {"$sum": "$total_revenue"}
                        }
                    },
                    {"$sort": {"total_revenue": -1}},
                    {"$limit": limit}
                ]
                
                top_products = list(collection.aggregate(product_pipeline))
                top_product_names = [prod["_id"] for prod in top_products]
                
                # Now get weekly trend data for these top products
                pipeline = [
                    {
                        "$match": {
                            **date_filter,
                            "product_name": {"$in": top_product_names}
                        }
                    },
                    {
                        "$project": {
                            "week_label": 1,
                            "iso_week": 1,
                            "product_name": 1,
                            "total_revenue": 1,
                            "total_quantity_sold": 1,
                            "total_transactions": 1,
                            "start_date": 1,
                            "_id": 0
                        }
                    },
                    {"$sort": {"start_date": 1, "product_name": 1}}
                ]
                
                result = list(collection.aggregate(pipeline))
                logger.info(f"Weekly product trend query returned {len(result)} documents")
                
                # Group by product and create multiple line series
                product_data = {}
                time_labels = set()
                
                for item in result:
                    product = item.get('product_name', '')
                    time_label = item.get('week_label', f"Week {item.get('iso_week', '')}")
                    revenue = item.get('total_revenue', 0)
                    
                    if product not in product_data:
                        product_data[product] = {}
                    product_data[product][time_label] = revenue
                    time_labels.add(time_label)
                
                # Sort time labels chronologically
                time_labels = sorted(time_labels)
                
                # Create chart data with multiple series
                chart_data = []
                for product, data in product_data.items():
                    y_values = [data.get(label, 0) for label in time_labels]
                    chart_data.append({
                        'x': time_labels,
                        'y': y_values,
                        'type': 'line',
                        'name': f'{product}',
                        'mode': 'lines+markers'
                    })
                
            elif interval.lower() == 'daily':
                # Use daily product data
                collection = db['sales_by_product_day']
                
                # First get top products by total revenue for the period
                product_pipeline = [
                    {"$match": date_filter},
                    {
                        "$group": {
                            "_id": "$product_name",
                            "total_revenue": {"$sum": "$total_revenue"}
                        }
                    },
                    {"$sort": {"total_revenue": -1}},
                    {"$limit": limit}
                ]
                
                top_products = list(collection.aggregate(product_pipeline))
                top_product_names = [prod["_id"] for prod in top_products]
                
                # Now get daily trend data for these top products
                pipeline = [
                    {
                        "$match": {
                            **date_filter,
                            "product_name": {"$in": top_product_names}
                        }
                    },
                    {
                        "$project": {
                            "display_date": 1,
                            "date": 1,
                            "product_name": 1,
                            "total_revenue": 1,
                            "total_quantity_sold": 1,
                            "total_transactions": 1,
                            "_id": 0
                        }
                    },
                    {"$sort": {"date": 1, "product_name": 1}},
                    {"$limit": 300}  # Limit to 300 days for performance
                ]
                
                result = list(collection.aggregate(pipeline))
                logger.info(f"Daily product trend query returned {len(result)} documents")
                
                # Group by product and create multiple line series
                product_data = {}
                time_labels = set()
                
                for item in result:
                    product = item.get('product_name', '')
                    time_label = item.get('display_date', '')
                    revenue = item.get('total_revenue', 0)
                    
                    if product not in product_data:
                        product_data[product] = {}
                    product_data[product][time_label] = revenue
                    time_labels.add(time_label)
                
                # Sort time labels chronologically
                time_labels = sorted(time_labels)
                
                # Create chart data with multiple series
                chart_data = []
                for product, data in product_data.items():
                    y_values = [data.get(label, 0) for label in time_labels]
                    chart_data.append({
                        'x': time_labels,
                        'y': y_values,
                        'type': 'line',
                        'name': f'{product}',
                        'mode': 'lines+markers'
                    })
            
            else:
                chart_data = []
            
            # Add product filter if specified after getting top products
            if categories and categories != ['all']:
                # Filter chart_data to only include specified products/categories
                chart_data = [series for series in chart_data if any(cat.lower() in series['name'].lower() for cat in categories)]
            
            mongo_conn.disconnect()
            
            # Create title based on date range or year
            if start_date and end_date:
                title = f'Product Performance Trends - {start_date} to {end_date} ({interval.title()})'
            else:
                title = f'Product Performance Trends - {year} ({interval.title()})'
            
            return {
                'success': True,
                'chart_type': 'line',
                'data': chart_data,
                'title': title,
                'start_date': start_date,
                'end_date': end_date,
                'year': year,
                'interval': interval,
                'top_products': len(chart_data)
            }
            
        except Exception as e:
            logger.error(f"Error getting product trend: {e}")
            return {'error': str(e)}, 500

@ns_chart.route('/payment-trend')
class PaymentTrendChart(Resource):
    def get(self):
        """Payment Method Trend Line Chart with Date Range Support"""
        try:
            # Get date range parameters or fall back to year
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            year = request.args.get('year', 2025, type=int)
            interval = request.args.get('interval', 'monthly')  # monthly, weekly, daily
            
            mongo_conn, db = get_mongo_connection()
            if db is None:
                return {'error': 'Database connection failed'}, 500
            
            # Build date filter similar to other endpoints
            date_filter = {}
            if start_date and end_date:
                from datetime import datetime
                start_dt = datetime.fromisoformat(start_date)
                end_dt = datetime.fromisoformat(end_date)
                
                if interval.lower() == 'monthly':
                    # For monthly data, filter by year and month
                    date_filter = {
                        "$and": [
                            {
                                "$or": [
                                    {"year": {"$gt": start_dt.year}},
                                    {
                                        "$and": [
                                            {"year": start_dt.year},
                                            {"month": {"$gte": start_dt.month}}
                                        ]
                                    }
                                ]
                            },
                            {
                                "$or": [
                                    {"year": {"$lt": end_dt.year}},
                                    {
                                        "$and": [
                                            {"year": end_dt.year},
                                            {"month": {"$lte": end_dt.month}}
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                else:
                    # For weekly/daily data, filter by date field
                    if interval.lower() == 'weekly':
                        date_filter = {
                            "$or": [
                                {"start_date": {"$gte": start_dt, "$lte": end_dt}},
                                {"end_date": {"$gte": start_dt, "$lte": end_dt}},
                                {"$and": [
                                    {"start_date": {"$lte": start_dt}},
                                    {"end_date": {"$gte": end_dt}}
                                ]}
                            ]
                        }
                    else:  # daily
                        date_filter = {
                            "date": {
                                "$gte": start_dt,
                                "$lte": end_dt
                            }
                        }
            else:
                # Fallback to year filter
                date_filter = {"year": year}
            
            if interval.lower() == 'monthly':
                # Use monthly payment data
                collection = db['payment_by_month']
                
                # First get top payment methods by total sales for the period
                payment_pipeline = [
                    {"$match": date_filter},
                    {
                        "$group": {
                            "_id": "$payment_method",
                            "total_sales": {"$sum": "$total_sales"}
                        }
                    },
                    {"$sort": {"total_sales": -1}},
                    {"$limit": 6}  # Top 6 payment methods for better visualization
                ]
                
                top_payments = list(collection.aggregate(payment_pipeline))
                top_payment_names = [payment["_id"] for payment in top_payments]
                
                # Now get monthly trend data for these top payment methods
                pipeline = [
                    {
                        "$match": {
                            **date_filter,
                            "payment_method": {"$in": top_payment_names}
                        }
                    },
                    {
                        "$project": {
                            "month": 1,
                            "month_name": 1,
                            "year": 1,
                            "payment_method": 1,
                            "total_sales": 1,
                            "total_transactions": 1,
                            "_id": 0
                        }
                    },
                    {"$sort": {"year": 1, "month": 1, "payment_method": 1}}
                ]
                
                result = list(collection.aggregate(pipeline))
                
                # Group by payment method and create multiple line series
                payment_data = {}
                time_labels = set()
                
                for item in result:
                    payment_method = item.get('payment_method', '')
                    time_label = item.get('month_name', f"Month {item.get('month', '')}")
                    sales = item.get('total_sales', 0)
                    
                    if payment_method not in payment_data:
                        payment_data[payment_method] = {}
                    payment_data[payment_method][time_label] = sales
                    time_labels.add(time_label)
                
                # Sort time labels chronologically
                month_order = ["January", "February", "March", "April", "May", "June",
                              "July", "August", "September", "October", "November", "December"]
                time_labels = sorted(time_labels, key=lambda x: month_order.index(x) if x in month_order else 999)
                
                # Create chart data with multiple series
                chart_data = []
                for payment_method, data in payment_data.items():
                    y_values = [data.get(label, 0) for label in time_labels]
                    chart_data.append({
                        'x': time_labels,
                        'y': y_values,
                        'type': 'line',
                        'name': f'{payment_method}',
                        'mode': 'lines+markers'
                    })
                
                if not chart_data:
                    chart_data = [{'x': [], 'y': [], 'type': 'line', 'name': 'No Data', 'mode': 'lines+markers'}]
                
            elif interval.lower() == 'weekly':
                # Use weekly payment data
                collection = db['payment_by_week']
                pipeline = [
                    {"$match": date_filter},
                    {
                        "$group": {
                            "_id": "$payment_method",
                            "total_sales": {"$sum": "$total_sales"},
                            "total_transactions": {"$sum": "$total_transactions"},
                            "weeks": {"$push": {"week": "$week_label", "sales": "$total_sales"}}
                        }
                    },
                    {
                        "$project": {
                            "payment_method": "$_id",
                            "total_sales": 1,
                            "total_transactions": 1,
                            "weeks": 1,
                            "_id": 0
                        }
                    },
                    {"$sort": {"total_sales": -1}},
                    {"$limit": 10}  # Top 10 payment methods
                ]
                
                result = list(collection.aggregate(pipeline))
                logger.info(f"Weekly payment trend query returned {len(result)} payment methods")
                
                # Format for line chart - showing top payment methods over time
                if result:
                    # Get the top payment method for line chart
                    top_payment = result[0]
                    chart_data = {
                        'x': [week['week'] for week in top_payment.get('weeks', [])],
                        'y': [week['sales'] for week in top_payment.get('weeks', [])],
                        'type': 'line',
                        'name': f"{top_payment.get('payment_method', 'Top Payment Method')} (Weekly)",
                        'mode': 'lines+markers'
                    }
                else:
                    chart_data = {'x': [], 'y': [], 'type': 'line', 'name': 'No Data', 'mode': 'lines+markers'}
                
            elif interval.lower() == 'daily':
                # Use daily payment data
                collection = db['payment_by_day']
                pipeline = [
                    {"$match": date_filter},
                    {
                        "$group": {
                            "_id": "$payment_method",
                            "total_sales": {"$sum": "$total_sales"},
                            "total_transactions": {"$sum": "$total_transactions"},
                            "days": {"$push": {"date": "$display_date", "sales": "$total_sales"}}
                        }
                    },
                    {
                        "$project": {
                            "payment_method": "$_id",
                            "total_sales": 1,
                            "total_transactions": 1,
                            "days": 1,
                            "_id": 0
                        }
                    },
                    {"$sort": {"total_sales": -1}},
                    {"$limit": 10}  # Top 10 payment methods
                ]
                
                result = list(collection.aggregate(pipeline))
                logger.info(f"Daily payment trend query returned {len(result)} payment methods")
                
                # Format for line chart - showing top payment methods over time  
                if result:
                    # Get the top payment method for line chart
                    top_payment = result[0]
                    # Limit to 30 days for readability
                    days_data = top_payment.get('days', [])[:30]
                    chart_data = {
                        'x': [day['date'] for day in days_data],
                        'y': [day['sales'] for day in days_data],
                        'type': 'line',
                        'name': f"{top_payment.get('payment_method', 'Top Payment Method')} (Daily)",
                        'mode': 'lines+markers'
                    }
                else:
                    chart_data = {'x': [], 'y': [], 'type': 'line', 'name': 'No Data', 'mode': 'lines+markers'}
            
            else:
                chart_data = {'x': [], 'y': [], 'type': 'line', 'name': 'Invalid Interval', 'mode': 'lines+markers'}
            
            mongo_conn.disconnect()
            
            # Create title based on date range or year
            if start_date and end_date:
                title = f'Payment Method Trends - {start_date} to {end_date} ({interval.title()})'
            else:
                title = f'Payment Method Trends - {year} ({interval.title()})'
            
            return {
                'success': True,
                'chart_type': 'line',
                'data': chart_data,
                'title': title,
                'start_date': start_date,
                'end_date': end_date,
                'year': year,
                'interval': interval
            }
            
        except Exception as e:
            logger.error(f"Error getting payment trend: {e}")
            return {'error': str(e)}, 500

@ns_chart.route('/revenue-candlestick')
class RevenueCandlestickChart(Resource):
    def get(self):
        """Revenue Candlestick Chart - OHLC data by different intervals"""
        try:
            # Get date range parameters or fall back to year
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            year = request.args.get('year', 2025, type=int)  # Fallback to year if no date range
            interval = request.args.get('interval', 'monthly')  # monthly, weekly, daily
            
            mongo_conn, db = get_mongo_connection()
            if db is None:
                return {'error': 'Database connection failed'}, 500
            
            # Build date filter
            date_filter = {}
            if start_date and end_date:
                from datetime import datetime
                start_dt = datetime.fromisoformat(start_date)
                end_dt = datetime.fromisoformat(end_date)
                
                if interval.lower() == 'monthly':
                    # For monthly data, filter by year and month
                    date_filter = {
                        "$and": [
                            {
                                "$or": [
                                    {"year": {"$gt": start_dt.year}},
                                    {
                                        "$and": [
                                            {"year": start_dt.year},
                                            {"month": {"$gte": start_dt.month}}
                                        ]
                                    }
                                ]
                            },
                            {
                                "$or": [
                                    {"year": {"$lt": end_dt.year}},
                                    {
                                        "$and": [
                                            {"year": end_dt.year},
                                            {"month": {"$lte": end_dt.month}}
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                else:
                    # For weekly/daily data, filter by start_date and end_date range overlap
                    date_filter = {
                        "$or": [
                            {"start_date": {"$gte": start_dt, "$lte": end_dt}},
                            {"end_date": {"$gte": start_dt, "$lte": end_dt}},
                            {
                                "$and": [
                                    {"start_date": {"$lte": start_dt}},
                                    {"end_date": {"$gte": end_dt}}
                                ]
                            }
                        ]
                    }
            else:
                # Fallback to year filter
                if interval.lower() == 'monthly':
                    date_filter = {"year": year}
                else:
                    date_filter = {"year": year}
            
            if interval.lower() == 'monthly':
                # Use monthly data
                collection = db['sales_by_month']
                pipeline = [
                    {"$match": date_filter},
                    {
                        "$project": {
                            "month": 1,
                            "month_name": 1,
                            "total_sales": 1,
                            "total_transactions": 1,
                            "_id": 0
                        }
                    },
                    {"$sort": {"month": 1}}
                ]
                
                result = list(collection.aggregate(pipeline))
                
                # Create OHLC data from monthly sales with realistic 10-20% ranges
                ohlc_data = []
                for i, item in enumerate(result):
                    sales = item.get('total_sales', 0)
                    
                    # Skip if sales is 0 or negative
                    if sales <= 0:
                        continue
                    
                    # Monthly: 10-20% range - use sales as base value
                    # Generate random but consistent values using index as seed
                    random.seed(i + 1000)  # Different seed for monthly
                    
                    # Random range between 10-20%
                    range_percent = 0.10 + (random.random() * 0.08)  # 10-18%
                    
                    # Create realistic OHLC around the sales value
                    # Open: random within ±5% of sales
                    open_offset = (random.random() - 0.5) * 0.10  # ±5%
                    open_val = sales * (1.0 + open_offset)
                    
                    # Close: random within ±5% of sales  
                    close_offset = (random.random() - 0.5) * 0.10  # ±5%
                    close_val = sales * (1.0 + close_offset)
                    
                    # High: highest of open/close + additional range
                    high_base = max(open_val, close_val)
                    high_val = high_base * (1.0 + range_percent * 0.6)  # Add 60% of range above
                    
                    # Low: lowest of open/close - additional range
                    low_base = min(open_val, close_val)
                    low_val = low_base * (1.0 - range_percent * 0.4)  # Subtract 40% of range below
                    
                    # Ensure no negative values
                    low_val = max(low_val, sales * 0.1)  # Never go below 10% of sales
                    
                    # Final validation: High >= Open,Close and Low <= Open,Close
                    high_val = max(high_val, open_val, close_val)
                    low_val = min(low_val, open_val, close_val)
                    
                    ohlc_data.append({
                        'x': item.get('month_name', f"Month {item.get('month', '')}"),
                        'open': round(open_val, 2),
                        'high': round(high_val, 2),
                        'low': round(low_val, 2),
                        'close': round(close_val, 2)
                    })
                    
            elif interval.lower() == 'weekly':
                # Use pre-aggregated weekly data
                collection = db['sales_by_week']
                
                # Debug: Log the filter being used
                logger.info(f"Weekly data filter: {date_filter}")
                
                pipeline = [
                    {"$match": date_filter},
                    {
                        "$project": {
                            "week_label": 1,
                            "iso_week": 1,
                            "start_date": 1,
                            "end_date": 1,
                            "total_sales": 1,
                            "total_transactions": 1,
                            "avg_transaction": 1,
                            "min_transaction": 1,
                            "max_transaction": 1,
                            "_id": 0
                        }
                    },
                    # Additional deduplication safeguard - group by week_label and take max values
                    {
                        "$group": {
                            "_id": "$week_label",
                            "iso_week": {"$first": "$iso_week"},
                            "start_date": {"$min": "$start_date"},
                            "end_date": {"$max": "$end_date"},
                            "total_sales": {"$sum": "$total_sales"},  # Sum if there are any duplicates
                            "total_transactions": {"$sum": "$total_transactions"},
                            "avg_transaction": {"$avg": "$avg_transaction"},
                            "min_transaction": {"$min": "$min_transaction"},
                            "max_transaction": {"$max": "$max_transaction"}
                        }
                    },
                    {
                        "$project": {
                            "week_label": "$_id",
                            "iso_week": 1,
                            "start_date": 1,
                            "end_date": 1,
                            "total_sales": 1,
                            "total_transactions": 1,
                            "avg_transaction": 1,
                            "min_transaction": 1,
                            "max_transaction": 1,
                            "_id": 0
                        }
                    },
                    {"$sort": {"start_date": 1}}  # Sort by start_date instead of iso_week
                ]
                
                result = list(collection.aggregate(pipeline))
                logger.info(f"Weekly data query returned {len(result)} documents")
                
                # Create OHLC data from weekly sales with realistic 5-10% ranges
                ohlc_data = []
                for i, item in enumerate(result):
                    sales = item.get('total_sales', 0)
                    
                    # Skip if sales is 0 or negative
                    if sales <= 0:
                        continue
                    
                    # Weekly: 5-10% range - use sales as base value
                    # Generate random but consistent values using index as seed
                    random.seed(i + 2000)  # Different seed for weekly
                    
                    # Random range between 5-10%
                    range_percent = 0.05 + (random.random() * 0.05)  # 5-10%
                    
                    # Create realistic OHLC around the sales value
                    # Open: random within ±3% of sales
                    open_offset = (random.random() - 0.5) * 0.06  # ±3%
                    open_val = sales * (1.0 + open_offset)
                    
                    # Close: random within ±3% of sales  
                    close_offset = (random.random() - 0.5) * 0.06  # ±3%
                    close_val = sales * (1.0 + close_offset)
                    
                    # High: highest of open/close + additional range
                    high_base = max(open_val, close_val)
                    high_val = high_base * (1.0 + range_percent * 0.6)  # Add 60% of range above
                    
                    # Low: lowest of open/close - additional range
                    low_base = min(open_val, close_val)
                    low_val = low_base * (1.0 - range_percent * 0.4)  # Subtract 40% of range below
                    
                    # Ensure no negative values
                    low_val = max(low_val, sales * 0.2)  # Never go below 20% of sales
                    
                    # Final validation: High >= Open,Close and Low <= Open,Close
                    high_val = max(high_val, open_val, close_val)
                    low_val = min(low_val, open_val, close_val)
                    
                    week_label = item.get('week_label', f"Week {item.get('iso_week', i+1)}")
                    ohlc_data.append({
                        'x': week_label,
                        'open': round(open_val, 2),
                        'high': round(high_val, 2),
                        'low': round(low_val, 2),
                        'close': round(close_val, 2)
                    })
                    
            elif interval.lower() == 'daily':
                # Use pre-aggregated daily data
                collection = db['sales_by_day']
                
                # For daily data, override the date_filter to use the 'date' field properly
                if start_date and end_date:
                    daily_filter = {"date": {"$gte": start_dt, "$lte": end_dt}}
                else:
                    daily_filter = {"year": year}
                
                logger.info(f"Daily data filter: {daily_filter}")
                
                pipeline = [
                    {"$match": daily_filter},
                    {
                        "$project": {
                            "display_date": 1,
                            "date": 1,
                            "total_sales": 1,
                            "total_transactions": 1,
                            "avg_transaction": 1,
                            "min_transaction": 1,
                            "max_transaction": 1,
                            "_id": 0
                        }
                    },
                    {"$sort": {"date": 1}},
                    {"$limit": 60}  # Limit to 60 days
                ]
                
                result = list(collection.aggregate(pipeline))
                logger.info(f"Daily data query returned {len(result)} documents")
                
                # Create OHLC data from daily sales with realistic 2-5% ranges
                ohlc_data = []
                for i, item in enumerate(result):
                    sales = item.get('total_sales', 0)
                    
                    # Skip if sales is 0 or negative
                    if sales <= 0:
                        continue
                    
                    # Daily: 2-5% range - use sales as base value
                    # Generate random but consistent values using index as seed
                    random.seed(i + 3000)  # Different seed for daily
                    
                    # Random range between 2-5%
                    range_percent = 0.02 + (random.random() * 0.03)  # 2-5%
                    
                    # Create realistic OHLC around the sales value
                    # Open: random within ±1.5% of sales
                    open_offset = (random.random() - 0.5) * 0.03  # ±1.5%
                    open_val = sales * (1.0 + open_offset)
                    
                    # Close: random within ±1.5% of sales  
                    close_offset = (random.random() - 0.5) * 0.03  # ±1.5%
                    close_val = sales * (1.0 + close_offset)
                    
                    # High: highest of open/close + additional range
                    high_base = max(open_val, close_val)
                    high_val = high_base * (1.0 + range_percent * 0.6)  # Add 60% of range above
                    
                    # Low: lowest of open/close - additional range
                    low_base = min(open_val, close_val)
                    low_val = low_base * (1.0 - range_percent * 0.4)  # Subtract 40% of range below
                    
                    # Ensure no negative values
                    low_val = max(low_val, sales * 0.5)  # Never go below 50% of sales
                    
                    # Final validation: High >= Open,Close and Low <= Open,Close
                    high_val = max(high_val, open_val, close_val)
                    low_val = min(low_val, open_val, close_val)
                    
                    display_date = item.get('display_date', f"Day {i+1}")
                    ohlc_data.append({
                        'x': display_date,
                        'open': round(open_val, 2),
                        'high': round(high_val, 2),
                        'low': round(low_val, 2),
                        'close': round(close_val, 2)
                    })
            
            else:
                ohlc_data = []
            
            mongo_conn.disconnect()
            
            # Create title based on date range or year
            if start_date and end_date:
                title = f'Revenue Candlestick - {start_date} to {end_date} ({interval.title()})'
            else:
                title = f'Revenue Candlestick - {year} ({interval.title()})'
            
            return {
                'success': True,
                'chart_type': 'candlestick',
                'data': ohlc_data,
                'title': title,
                'start_date': start_date,
                'end_date': end_date,
                'year': year,
                'interval': interval
            }
            
        except Exception as e:
            logger.error(f"Error getting revenue candlestick: {e}")
            return {'error': str(e)}, 500

@ns_chart.route('/product-time-analysis')
class ProductTimeAnalysisChart(Resource):
    def get(self):
        """Product Sales by Time Period - Stacked Bar Chart"""
        try:
            # Get parameters
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            interval = request.args.get('interval', 'monthly')
            limit = request.args.get('limit', 10, type=int)
            
            mongo_conn, db = get_mongo_connection()
            if db is None:
                return {'error': 'Database connection failed'}, 500
            
            # Build date filter
            date_filter = {}
            
            if start_date and end_date:
                from datetime import datetime
                start_dt = datetime.fromisoformat(start_date)
                end_dt = datetime.fromisoformat(end_date)
                
                if interval.lower() == 'monthly':
                    date_filter = {
                        "$and": [
                            {
                                "$or": [
                                    {"year": {"$gt": start_dt.year}},
                                    {
                                        "$and": [
                                            {"year": start_dt.year},
                                            {"month": {"$gte": start_dt.month}}
                                        ]
                                    }
                                ]
                            },
                            {
                                "$or": [
                                    {"year": {"$lt": end_dt.year}},
                                    {
                                        "$and": [
                                            {"year": end_dt.year},
                                            {"month": {"$lte": end_dt.month}}
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                elif interval.lower() == 'weekly':
                    date_filter = {
                        "$or": [
                            {"start_date": {"$gte": start_dt, "$lte": end_dt}},
                            {"end_date": {"$gte": start_dt, "$lte": end_dt}}
                        ]
                    }
                else:  # daily
                    date_filter = {
                        "date": {
                            "$gte": start_dt,
                            "$lte": end_dt
                        }
                    }
            else:
                date_filter = {"year": 2025}
            
            # Use appropriate collection based on interval
            if interval.lower() == 'monthly':
                collection = db['sales_by_product_month']
            elif interval.lower() == 'weekly':
                collection = db['sales_by_product_week']
            else:  # daily
                collection = db['sales_by_product_day']
            
            # First, get top products by total revenue
            top_products_pipeline = [
                {"$match": date_filter},
                {
                    "$group": {
                        "_id": "$product_name",
                        "total_revenue": {"$sum": "$total_revenue"}
                    }
                },
                {"$sort": {"total_revenue": -1}},
                {"$limit": limit}
            ]
            
            top_products_result = list(collection.aggregate(top_products_pipeline))
            top_product_names = [item['_id'] for item in top_products_result]
            
            # Get time series data for top products
            time_series_filter = {
                **date_filter,
                "product_name": {"$in": top_product_names}
            }
            
            pipeline = [
                {"$match": time_series_filter},
                {
                    "$addFields": {
                        "time_label": {
                            "$switch": {
                                "branches": [
                                    {
                                        "case": {"$eq": [interval.lower(), "monthly"]},
                                        "then": {
                                            "$concat": [
                                                {"$toString": "$year"},
                                                "-",
                                                {
                                                    "$cond": {
                                                        "if": {"$lt": ["$month", 10]},
                                                        "then": {"$concat": ["0", {"$toString": "$month"}]},
                                                        "else": {"$toString": "$month"}
                                                    }
                                                }
                                            ]
                                        }
                                    },
                                    {
                                        "case": {"$eq": [interval.lower(), "weekly"]},
                                        "then": "$week_label"
                                    },
                                    {
                                        "case": {"$eq": [interval.lower(), "daily"]},
                                        "then": "$display_date"
                                    }
                                ],
                                "default": "$month_name"
                            }
                        }
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "time_label": "$time_label",
                            "product_name": "$product_name"
                        },
                        "total_revenue": {"$sum": "$total_revenue"}
                    }
                },
                {"$sort": {"_id.time_label": 1}}
            ]
            
            result = list(collection.aggregate(pipeline))
            
            # Transform data for stacked bar chart
            # Group by product (each product becomes a series)
            products = {}
            time_periods = set()
            
            for item in result:
                product_name = item['_id']['product_name']
                time_label = item['_id']['time_label']
                revenue = item['total_revenue']
                
                time_periods.add(time_label)
                
                if product_name not in products:
                    products[product_name] = {}
                
                products[product_name][time_label] = revenue
            
            # Convert to chart format
            time_periods = sorted(list(time_periods))
            chart_data = []
            
            for product_name in top_product_names:
                if product_name in products:
                    series_data = {
                        'x': time_periods,
                        'y': [products[product_name].get(period, 0) for period in time_periods],
                        'name': product_name,
                        'type': 'bar'
                    }
                    chart_data.append(series_data)
            
            chart_type = 'stacked_bar'
            
            title = f"Top {limit} Products Sales by {interval.title()} Period"
            
            mongo_conn.disconnect()
            
            return {
                'success': True,
                'data': chart_data,
                'chart_type': chart_type,
                'title': title,
                'filters': {
                    'interval': interval,
                    'limit': limit,
                    'start_date': start_date,
                    'end_date': end_date
                }
            }
            
        except Exception as e:
            logger.error(f"Error in product-location-analysis: {e}")
            return {'error': str(e)}, 500

@ns_chart.route('/transaction-volume')
class TransactionVolumeChart(Resource):
    def get(self):
        """Transaction Volume Line Chart with Date Range Support"""
        try:
            # Get date range parameters or fall back to year
            start_date = request.args.get('start_date')
            end_date = request.args.get('end_date')
            year = request.args.get('year', 2025, type=int)
            interval = request.args.get('interval', 'monthly')  # monthly, weekly, daily
            
            mongo_conn, db = get_mongo_connection()
            if db is None:
                return {'error': 'Database connection failed'}, 500
            
            # Build date filter similar to candlestick endpoint
            date_filter = {}
            if start_date and end_date:
                from datetime import datetime
                start_dt = datetime.fromisoformat(start_date)
                end_dt = datetime.fromisoformat(end_date)
                
                if interval.lower() == 'monthly':
                    # For monthly data, filter by year and month
                    date_filter = {
                        "$and": [
                            {
                                "$or": [
                                    {"year": {"$gt": start_dt.year}},
                                    {
                                        "$and": [
                                            {"year": start_dt.year},
                                            {"month": {"$gte": start_dt.month}}
                                        ]
                                    }
                                ]
                            },
                            {
                                "$or": [
                                    {"year": {"$lt": end_dt.year}},
                                    {
                                        "$and": [
                                            {"year": end_dt.year},
                                            {"month": {"$lte": end_dt.month}}
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                else:
                    # For weekly/daily data, filter by date field
                    if interval.lower() == 'weekly':
                        date_filter = {
                            "$or": [
                                {"start_date": {"$gte": start_dt, "$lte": end_dt}},
                                {"end_date": {"$gte": start_dt, "$lte": end_dt}},
                                {"$and": [
                                    {"start_date": {"$lte": start_dt}},
                                    {"end_date": {"$gte": end_dt}}
                                ]}
                            ]
                        }
                    else:  # daily
                        date_filter = {
                            "date": {
                                "$gte": start_dt,
                                "$lte": end_dt
                            }
                        }
            else:
                # Fallback to year filter
                date_filter = {"year": year}
            
            if interval.lower() == 'monthly':
                # Use monthly data
                collection = db['sales_by_month']
                pipeline = [
                    {"$match": date_filter},
                    {
                        "$project": {
                            "month": 1,
                            "month_name": 1,
                            "year": 1,
                            "total_transactions": 1,
                            "_id": 0
                        }
                    },
                    {"$sort": {"year": 1, "month": 1}}
                ]
                
                result = list(collection.aggregate(pipeline))
                
                # Format for line chart
                chart_data = {
                    'x': [item.get('month_name', f"Month {item.get('month', '')}") for item in result],
                    'y': [item.get('total_transactions', 0) for item in result],
                    'type': 'line',
                    'name': 'Transaction Volume (Monthly)',
                    'mode': 'lines+markers',
                    'fill': 'tonexty'
                }
                
            elif interval.lower() == 'weekly':
                # Use weekly data
                collection = db['sales_by_week']
                pipeline = [
                    {"$match": date_filter},
                    {
                        "$group": {
                            "_id": "$week_label",
                            "total_transactions": {"$sum": "$total_transactions"},
                            "iso_week": {"$first": "$iso_week"},
                            "start_date": {"$first": "$start_date"}
                        }
                    },
                    {
                        "$project": {
                            "week_label": "$_id",
                            "total_transactions": 1,
                            "iso_week": 1,
                            "start_date": 1,
                            "_id": 0
                        }
                    },
                    {"$sort": {"start_date": 1}}
                ]
                
                result = list(collection.aggregate(pipeline))
                logger.info(f"Weekly transaction volume query returned {len(result)} documents")
                
                # Format for line chart
                chart_data = {
                    'x': [item.get('week_label', f"Week {item.get('iso_week', '')}") for item in result],
                    'y': [item.get('total_transactions', 0) for item in result],
                    'type': 'line',
                    'name': 'Transaction Volume (Weekly)',
                    'mode': 'lines+markers',
                    'fill': 'tonexty'
                }
                
            elif interval.lower() == 'daily':
                # Use daily data
                collection = db['sales_by_day']
                pipeline = [
                    {"$match": date_filter},
                    {
                        "$project": {
                            "display_date": 1,
                            "date": 1,
                            "total_transactions": 1,
                            "_id": 0
                        }
                    },
                    {"$sort": {"date": 1}},
                    {"$limit": 60}  # Limit to 60 days
                ]
                
                result = list(collection.aggregate(pipeline))
                logger.info(f"Daily transaction volume query returned {len(result)} documents")
                
                # Format for line chart
                chart_data = {
                    'x': [item.get('display_date', f"Day {i+1}") for i, item in enumerate(result)],
                    'y': [item.get('total_transactions', 0) for item in result],
                    'type': 'line',
                    'name': 'Transaction Volume (Daily)',
                    'mode': 'lines+markers',
                    'fill': 'tonexty'
                }
            
            else:
                chart_data = {
                    'x': [],
                    'y': [],
                    'type': 'line',
                    'name': 'Transaction Volume',
                    'mode': 'lines+markers'
                }
            
            mongo_conn.disconnect()
            
            # Create title based on date range or year
            if start_date and end_date:
                title = f'Transaction Volume - {start_date} to {end_date} ({interval.title()})'
            else:
                title = f'Transaction Volume - {year} ({interval.title()})'
            
            return {
                'success': True,
                'chart_type': 'line',
                'data': chart_data,
                'title': title,
                'start_date': start_date,
                'end_date': end_date,
                'year': year,
                'interval': interval
            }
            
        except Exception as e:
            logger.error(f"Error getting transaction volume: {e}")
            return {'error': str(e)}, 500

@app.route('/chart-health')
def chart_health():
    """Chart API health check"""
    try:
        mongo_conn, db = get_mongo_connection()
        if db is not None:
            collections = db.list_collection_names()
            mongo_conn.disconnect()
            return {
                'status': 'healthy',
                'collections': len(collections),
                'available_endpoints': [
                    '/chart/sales-trend',
                    '/chart/location-performance', 
                    '/chart/product-trend',
                    '/chart/payment-trend',
                    '/chart/revenue-candlestick',
                    '/chart/transaction-volume'
                ]
            }
        else:
            return {'status': 'unhealthy', 'error': 'Database connection failed'}, 500
    except Exception as e:
        return {'status': 'unhealthy', 'error': str(e)}, 500

if __name__ == '__main__':
    port = 5004  # Different port for chart API
    print("🚀 Starting Chart API Server")
    print(f"📚 Chart docs available at: http://localhost:{port}/chart-docs")
    print(f"🏥 Health check at: http://localhost:{port}/chart-health")
    
    app.run(debug=True, host='0.0.0.0', port=port)