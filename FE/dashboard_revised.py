#!/usr/bin/env python3
"""
Revised Analytics Dashboard - Line & Candlestick Charts Only
Individual filters for each chart, separate API calls
"""

# Load environment variables first
import sys
import os
sys.path.append('..')
import load_env

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import json
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')
from chart_descriptions import show_chart_description

# Import MongoDB connection for location data
from mongodb_connection import MongoDBSSHConnection

# Page configuration
st.set_page_config(
    page_title="Tea Shop Analytics Dashboard - Revised",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)


# Custom CSS for professional dashboard
st.markdown("""
<style>
    .main > div {
        padding-top: 1rem;
    }
    
    .chart-container {
        background-color: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin-bottom: 2rem;
        border-left: 4px solid #1f77b4;
    }
    
    .filter-section {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1rem;
        border: 1px solid #e9ecef;
    }
    
    h1 {
        color: #1f77b4;
        font-size: 2.2rem;
        font-weight: 700;
        margin-bottom: 1rem;
        text-align: center;
    }
    
    h2 {
        color: #2c3e50;
        font-size: 1.6rem;
        font-weight: 600;
        margin-bottom: 1rem;
    }
    
    h3 {
        color: #34495e;
        font-size: 1.3rem;
        font-weight: 500;
        margin-bottom: 0.8rem;
    }
    
    .metric-row {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 2rem;
        text-align: center;
    }
    
    .stSelectbox > div > div > select {
        border: 2px solid #3498db;
        border-radius: 5px;
    }
    
    .stMultiSelect > div > div > div {
        border: 2px solid #3498db;
        border-radius: 5px;
    }
    
</style>
""", unsafe_allow_html=True)

# API Configuration
CHART_API_BASE_URL = f"http://localhost:5004"

@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_chart_data(endpoint, params=None, cache_key=None):
    """Fetch data from chart API with cache key support"""
    try:
        url = f"{CHART_API_BASE_URL}/chart/{endpoint}"
        response = requests.get(url, params=params or {}, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                return data
        return None
    except Exception as e:
        st.error(f"Error fetching chart data from {endpoint}: {e}")
        return None

@st.cache_data(ttl=3600)  # Cache for 1 hour
def get_location_options():
    """Get location options from master_locations collection"""
    try:
        mongo_conn = MongoDBSSHConnection()
        client = mongo_conn.connect()
        
        if not client:
            return ["All", "Kebun 0491", "Kebun 0050"]  # Fallback
        
        db = mongo_conn.get_database()
        
        # Get top locations by sales
        locations = list(db['master_locations'].find(
            {},
            {"location_name": 1, "_id": 0}
        ).sort("total_sales", -1).limit(20))  # Top 20 locations
        
        location_names = ["All"] + [loc['location_name'] for loc in locations]
        
        mongo_conn.disconnect()
        return location_names
        
    except Exception as e:
        # Fallback to hardcoded options if database fails
        return ["All", "Kebun 0491 Antapani Bandung", "Kebun 0050 Sukahati"]

@st.cache_data(ttl=3600)  # Cache for 1 hour  
def get_product_options():
    """Get product options from transaction_sales collection"""
    try:
        mongo_conn = MongoDBSSHConnection()
        client = mongo_conn.connect()
        
        if not client:
            return ["All", "Tea Product A", "Tea Product B"]  # Fallback
        
        db = mongo_conn.get_database()
        
        # Get top products by sales volume
        pipeline = [
            {"$match": {"Product Name": {"$ne": None, "$ne": ""}}},
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
                    "_id": "$Product Name",
                    "total_sales": {"$sum": "$total_numeric"},
                    "total_transactions": {"$sum": 1}
                }
            },
            {"$sort": {"total_sales": -1}},
            {"$limit": 30}  # Top 30 products
        ]
        
        result = list(db['transaction_sales'].aggregate(pipeline))
        product_names = ["All"] + [item['_id'] for item in result]
        
        mongo_conn.disconnect()
        return product_names
        
    except Exception as e:
        # Fallback to hardcoded options if database fails
        return ["All", "Green Tea", "Black Tea", "Oolong Tea"]

def create_fullscreen_product_time_analysis(params):
    """Create fullscreen product time analysis chart"""
    st.title("📊 Product Sales by Time Period - Full Screen")
    
    # Fetch data with same parameters
    chart_data = fetch_chart_data('product-time-analysis', params)
    
    if chart_data and chart_data.get('data'):
        data = chart_data['data']
        chart_type = chart_data.get('chart_type', 'bar')
        
        fig = go.Figure()
        
        if chart_type == 'heatmap' and isinstance(data, dict) and 'z' in data:
            # Heatmap for product x location analysis
            fig.add_trace(go.Heatmap(
                x=data.get('x', []),  # Time periods
                y=data.get('y', []),  # Products or Locations
                z=data.get('z', []),  # Sales values
                colorscale='Viridis',
                hovertemplate='<b>%{y}</b><br>Period: %{x}<br>Sales: Rp %{z:,.0f}<extra></extra>'
            ))
        elif isinstance(data, list) and len(data) > 0:
            # Multiple series bar/line chart
            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
            
            for i, series in enumerate(data):
                color = colors[i % len(colors)]
                if chart_type == 'line':
                    fig.add_trace(go.Scatter(
                        x=series.get('x', []),
                        y=series.get('y', []),
                        mode='lines+markers',
                        name=series.get('name', f'Series {i+1}'),
                        line=dict(color=color, width=3),
                        marker=dict(size=8)
                    ))
                else:  # bar chart
                    fig.add_trace(go.Bar(
                        x=series.get('x', []),
                        y=series.get('y', []),
                        name=series.get('name', f'Series {i+1}'),
                        marker_color=color
                    ))
            
            if chart_type == 'bar':
                fig.update_layout(barmode='group')
        
        fig.update_layout(
            title=chart_data.get('title', 'Product Sales Analysis by Location & Time'),
            height=800,
            showlegend=True,
            hovermode='closest',
            xaxis_title="Time Period",
            yaxis_title="Sales (Rp)",
            font=dict(size=18)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show metrics
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Date Range", f"{params.get('start_date', 'N/A')} to {params.get('end_date', 'N/A')}")
        with col2:
            st.metric("Location", params.get('location', 'All'))
        with col3:
            st.metric("Product", params.get('product', 'All'))
        with col4:
            st.metric("Grouping", params.get('interval', 'monthly').title())
        with col5:
            st.metric("Chart Type", chart_type.title())
    else:
        st.error("No data available for this analysis")

def create_product_time_analysis_chart():
    """Top Chart: Product Sales by Time Period (Stacked Bar)"""
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    
    # Header with fullscreen button
    col1, col2 = st.columns([4, 1])
    with col1:
        st.subheader("📊 Product Sales by Time Period")
        st.markdown("""
        <div style="background-color: #f0f8ff; padding: 0.5rem; border-radius: 5px; margin-bottom: 1rem; font-size: 0.85rem;">
        <b>📊 How to Read:</b> Stacked bar chart showing top products' sales over time. Each bar = time period, colored segments = individual products. 
        Use to identify <b>trending products</b>, <b>seasonal patterns</b>, and <b>product mix changes</b>.
        </div>
        """, unsafe_allow_html=True)
    with col2:
        analysis_fullscreen_btn = st.button("🔍 Fullscreen", key="analysis_fullscreen", help="View in fullscreen popup")
    
    # Individual filters in single row
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        analysis_start_date = st.date_input("Start Date", value=datetime(2025, 4, 1), key="analysis_start")
    with col2:
        analysis_end_date = st.date_input("End Date", value=datetime(2025, 6, 30), key="analysis_end")
    with col3:
        analysis_interval = st.selectbox("Grouping", ["Monthly", "Weekly", "Daily"], key="analysis_interval")
    with col4:
        top_products_limit = st.slider("Top N Products", 5, 20, 10, key="analysis_products_limit")
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Fetch data
    params = {
        'start_date': analysis_start_date.isoformat() if analysis_start_date else None,
        'end_date': analysis_end_date.isoformat() if analysis_end_date else None,
        'interval': analysis_interval.lower(),
        'limit': top_products_limit
    }
    
    # Show fullscreen in new tab if button clicked
    if analysis_fullscreen_btn:
        # Create URL with parameters
        base_url = "http://localhost:8501"
        param_parts = []
        for k, v in params.items():
            if v is not None:
                param_parts.append(f"{k}={v}")
        params_str = "&".join(param_parts)
        fullscreen_url = f"{base_url}?fullscreen=product_time_analysis&{params_str}"
        
        # Show link button to open in new tab
        st.markdown(f"""
        <a href="{fullscreen_url}" target="_blank" style="
            display: inline-block;
            padding: 0.5rem 1rem;
            background-color: #17becf;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            font-weight: bold;
            margin: 0.5rem 0;
        ">📊 Open Analysis Chart in New Tab</a>
        """, unsafe_allow_html=True)
    
    chart_data = fetch_chart_data('product-time-analysis', params)
    
    if chart_data and chart_data.get('data'):
        data = chart_data['data']
        chart_type = chart_data.get('chart_type', 'bar')
        
        fig = go.Figure()
        
        if chart_type == 'heatmap' and isinstance(data, dict) and 'z' in data:
            # Heatmap for complex analysis
            fig.add_trace(go.Heatmap(
                x=data.get('x', []),  # Time periods
                y=data.get('y', []),  # Products or Locations  
                z=data.get('z', []),  # Sales values
                colorscale='Viridis',
                hovertemplate='<b>%{y}</b><br>Period: %{x}<br>Sales: Rp %{z:,.0f}<extra></extra>'
            ))
        elif isinstance(data, list) and len(data) > 0:
            # Stacked bar chart for products by time
            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
            
            for i, series in enumerate(data):
                color = colors[i % len(colors)]
                fig.add_trace(go.Bar(
                    x=series.get('x', []),
                    y=series.get('y', []),
                    name=series.get('name', f'Product {i+1}'),
                    marker_color=color,
                    hovertemplate=f'<b>{series.get("name", "Product")}</b><br>Period: %{{x}}<br>Sales: Rp %{{y:,.0f}}<extra></extra>'
                ))
            
            # Use stacked bar mode
            fig.update_layout(
                barmode='stack',
                bargap=0.3
            )
        
        fig.update_layout(
            title=chart_data.get('title', 'Product Sales by Time Period'),
            height=500,
            showlegend=True,
            hovermode='x unified',
            xaxis_title="Time Period",
            yaxis_title="Sales (Rp)",
            font=dict(size=14)
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available for this analysis")
    
    st.markdown('</div>', unsafe_allow_html=True)

def create_fullscreen_sales_trend(params):
    """Create fullscreen sales trend chart"""
    st.title("📈 Sales Trend Analysis - Full Screen")
    
    # Fetch data with same parameters
    chart_data = fetch_chart_data('sales-trend', params)
    
    if chart_data and chart_data.get('data'):
        data = chart_data['data']
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=data.get('x', []),
            y=data.get('y', []),
            mode='lines+markers',
            name=data.get('name', 'Sales Trend'),
            line=dict(color='#1f77b4', width=4),
            marker=dict(size=12, color='#ff7f0e'),
            fill='tonexty'
        ))
        
        fig.update_layout(
            title=chart_data.get('title', 'Sales Trend Over Time'),
            height=700,
            showlegend=True,
            hovermode='x unified',
            xaxis_title="Time Period",
            yaxis_title="Sales (Rp)",
            font=dict(size=18)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Date Range", f"{params.get('start_date', 'N/A')} to {params.get('end_date', 'N/A')}")
        with col2:
            st.metric("Interval", params.get('interval', 'monthly').title())
        with col3:
            st.metric("Location", params.get('location', 'All'))
        with col4:
            st.metric("Data Points", len(data.get('x', [])))
    else:
        st.error("No data available for this chart")

def create_sales_trend_chart():
    """Chart 1: Sales Trend Line Chart"""
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    
    # Header with fullscreen button
    col1, col2 = st.columns([4, 1])
    with col1:
        st.subheader("📈 Sales Trend Over Time")
        st.markdown("""
        <div style="background-color: #f0f2f6; padding: 0.5rem; border-radius: 5px; margin-bottom: 1rem; font-size: 0.85rem;">
        <b>📈 How to Read:</b> Line chart showing sales trends across time periods. Filter by location to analyze branch performance. 
        Use to identify <b>growth patterns</b>, <b>seasonal trends</b>, and <b>performance dips</b>.
        </div>
        """, unsafe_allow_html=True)
    with col2:
        fullscreen_btn = st.button("🔍 Fullscreen", key="sales_fullscreen", help="View in fullscreen popup")
    
    # Individual filters for this chart
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        sales_start_date = st.date_input("Start Date", value=datetime(2025, 4, 1), key="sales_start")
    with col2:
        sales_end_date = st.date_input("End Date", value=datetime(2025, 6, 30), key="sales_end")
    with col3:
        sales_interval = st.selectbox("Grouping", ["Monthly", "Weekly", "Daily"], key="sales_interval")
    with col4:
        location_options = get_location_options()
        location_filter = st.selectbox(
            "Location", 
            options=location_options,  # Include "All" option
            index=0,  # Default to "All" (first option)
            key="sales_location",
            help="Select a location or 'All' for all locations."
        )
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Fetch data
    params = {
        'start_date': sales_start_date.isoformat() if sales_start_date else None,
        'end_date': sales_end_date.isoformat() if sales_end_date else None,
        'interval': sales_interval.lower(),
        'locations': [location_filter] if location_filter and location_filter != "All" else None
    }
    
    chart_data = fetch_chart_data('sales-trend', params)
    
    # Show fullscreen in new tab if button clicked
    if fullscreen_btn:
        # Create URL with parameters
        base_url = "http://localhost:8501"
        param_parts = []
        for k, v in params.items():
            if v is not None:
                if k == 'locations' and isinstance(v, list):
                    # Handle list parameters for locations - use single location for URL
                    if v:  # If list is not empty
                        param_parts.append(f"locations={v[0]}")
                else:
                    param_parts.append(f"{k}={v}")
        params_str = "&".join(param_parts)
        fullscreen_url = f"{base_url}?fullscreen=sales_trend&{params_str}"
        
        # Show link button to open in new tab
        st.markdown(f"""
        <a href="{fullscreen_url}" target="_blank" style="
            display: inline-block;
            padding: 0.5rem 1rem;
            background-color: #1f77b4;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            font-weight: bold;
            margin: 0.5rem 0;
        ">🔍 Open Fullscreen Chart in New Tab</a>
        """, unsafe_allow_html=True)
    
    if chart_data and chart_data.get('data'):
        data = chart_data['data']
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=data.get('x', []),
            y=data.get('y', []),
            mode='lines+markers',
            name=data.get('name', 'Sales Trend'),
            line=dict(color='#1f77b4', width=3),
            marker=dict(size=8, color='#ff7f0e'),
            fill='tonexty'
        ))
        
        fig.update_layout(
            title=chart_data.get('title', 'Sales Trend'),
            height=400,
            showlegend=True,
            hovermode='x unified',
            xaxis_title="Time Period",
            yaxis_title="Sales (Rp)"
        )
        
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': True, 'responsive': True})
    else:
        st.warning("No data available for sales trend")
    
    st.markdown('</div>', unsafe_allow_html=True)

def create_fullscreen_location_performance(params):
    """Create fullscreen location performance chart"""
    st.title("🏢 Location Performance Analysis - Full Screen")
    
    # Fetch data with same parameters
    chart_data = fetch_chart_data('location-performance', params)
    
    if chart_data and chart_data.get('data'):
        data = chart_data['data']
        
        fig = go.Figure()
        
        # Handle multiple series
        if isinstance(data, list) and len(data) > 0:
            colors = ['#2ca02c', '#d62728', '#ff7f0e', '#1f77b4', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
            
            for i, series in enumerate(data):
                color = colors[i % len(colors)]
                fig.add_trace(go.Bar(
                    x=series.get('x', []),
                    y=series.get('y', []),
                    name=series.get('name', f'Location {i+1}'),
                    marker_color=color,
                    hovertemplate=f'<b>{series.get("name", "Location")}</b><br>Period: %{{x}}<br>Sales: Rp %{{y:,.0f}}<extra></extra>'
                ))
            
            fig.update_layout(
                barmode='group',
                bargap=0.15,
                bargroupgap=0.1
            )
        
        fig.update_layout(
            title=chart_data.get('title', 'Location Performance Comparison'),
            height=700,
            showlegend=True,
            hovermode='closest',
            xaxis_title="Time Period",
            yaxis_title="Sales (Rp)",
            font=dict(size=18)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Date Range", f"{params.get('start_date', 'N/A')} to {params.get('end_date', 'N/A')}")
        with col2:
            st.metric("Interval", params.get('interval', 'monthly').title())
        with col3:
            st.metric("Top Locations", params.get('limit', 'N/A'))
        with col4:
            st.metric("Total Series", len(data) if isinstance(data, list) else 1)
    else:
        st.error("No data available for this chart")

def create_location_performance_chart():
    """Chart 2: Location Performance Chart"""
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    
    # Header with fullscreen button
    col1, col2 = st.columns([4, 1])
    with col1:
        st.subheader("🏪 Location Performance Comparison")
        st.markdown("""
        <div style="background-color: #f0f2f6; padding: 0.5rem; border-radius: 5px; margin-bottom: 1rem; font-size: 0.85rem;">
        <b>🏪 How to Read:</b> Grouped bar chart comparing sales performance across top locations by time period. 
        Each color represents a different location. Use to identify <b>best performing branches</b>, <b>regional trends</b>, and <b>location-specific seasonality</b>.
        </div>
        """, unsafe_allow_html=True)
    with col2:
        location_fullscreen_btn = st.button("🔍 Fullscreen", key="location_fullscreen", help="View in fullscreen popup")
    
    # Individual filters
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        location_start_date = st.date_input("Start Date", value=datetime(2025, 4, 1), key="location_start")
    with col2:
        location_end_date = st.date_input("End Date", value=datetime(2025, 6, 30), key="location_end")
    with col3:
        location_interval = st.selectbox("Grouping", ["Monthly", "Weekly", "Daily"], key="location_interval")
    with col4:
        location_limit = st.slider("Top N Locations", 5, 30, 15, key="location_limit")
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Fetch data
    params = {
        'start_date': location_start_date.isoformat() if location_start_date else None,
        'end_date': location_end_date.isoformat() if location_end_date else None,
        'interval': location_interval.lower(),
        'limit': location_limit
    }
    
    chart_data = fetch_chart_data('location-performance', params)
    
    # Show fullscreen in new tab if button clicked
    if location_fullscreen_btn:
        # Create URL with parameters
        base_url = "http://localhost:8501"
        params_str = "&".join([f"{k}={v}" for k, v in params.items() if v is not None])
        fullscreen_url = f"{base_url}?fullscreen=location_performance&{params_str}"
        
        # Show link button to open in new tab
        st.markdown(f"""
        <a href="{fullscreen_url}" target="_blank" style="
            display: inline-block;
            padding: 0.5rem 1rem;
            background-color: #2ca02c;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            font-weight: bold;
            margin: 0.5rem 0;
        ">🔍 Open Fullscreen Chart in New Tab</a>
        """, unsafe_allow_html=True)
    
    if chart_data and chart_data.get('data'):
        data = chart_data['data']
        
        fig = go.Figure()
        
        # Handle multiple series (new API structure) - Use Bar Chart for better visibility
        if isinstance(data, list) and len(data) > 0:
            colors = ['#2ca02c', '#d62728', '#ff7f0e', '#1f77b4', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
            
            # Get all time periods from first series
            if data[0].get('x'):
                time_periods = data[0]['x']
                
                # Create grouped bar chart
                for i, series in enumerate(data):
                    color = colors[i % len(colors)]
                    fig.add_trace(go.Bar(
                        x=series.get('x', []),
                        y=series.get('y', []),
                        name=series.get('name', f'Location {i+1}'),
                        marker_color=color,
                        hovertemplate=f'<b>{series.get("name", "Location")}</b><br>Period: %{{x}}<br>Sales: Rp %{{y:,.0f}}<extra></extra>'
                    ))
                    
                # Update layout for grouped bar chart
                fig.update_layout(
                    barmode='group',
                    bargap=0.15,
                    bargroupgap=0.1
                )
        else:
            # Fallback for old API structure
            fig.add_trace(go.Bar(
                x=data.get('x', []) if hasattr(data, 'get') else [],
                y=data.get('y', []) if hasattr(data, 'get') else [],
                name='Location Performance',
                marker_color='#2ca02c'
            ))
        
        fig.update_layout(
            title=chart_data.get('title', 'Location Performance'),
            height=400,
            showlegend=True,
            hovermode='closest',
            xaxis_title="Location Index",
            yaxis_title="Sales (Rp)"
        )
        
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': True, 'responsive': True})
    else:
        st.warning("No data available for location performance")
    
    st.markdown('</div>', unsafe_allow_html=True)

def create_fullscreen_product_performance(params):
    """Create fullscreen product category chart"""
    st.title("🛍️ Product Category Analysis - Full Screen")
    
    # Fetch data with same parameters
    chart_data = fetch_chart_data('product-trend', params)
    
    if chart_data and chart_data.get('data'):
        data = chart_data['data']
        
        fig = go.Figure()
        
        # Handle multiple series
        if isinstance(data, list) and len(data) > 0:
            colors = ['#ff7f0e', '#1f77b4', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
            
            for i, series in enumerate(data):
                color = colors[i % len(colors)]
                fig.add_trace(go.Bar(
                    x=series.get('x', []),
                    y=series.get('y', []),
                    name=series.get('name', f'Product {i+1}'),
                    marker_color=color,
                    hovertemplate=f'<b>{series.get("name", "Product")}</b><br>Period: %{{x}}<br>Revenue: Rp %{{y:,.0f}}<extra></extra>'
                ))
            
            fig.update_layout(
                barmode='group',
                bargap=0.15,
                bargroupgap=0.1
            )
        
        fig.update_layout(
            title=chart_data.get('title', 'Product Category Performance Comparison'),
            height=700,
            showlegend=True,
            hovermode='closest',
            xaxis_title="Time Period",
            yaxis_title="Revenue (Rp)",
            font=dict(size=18)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Date Range", f"{params.get('start_date', 'N/A')} to {params.get('end_date', 'N/A')}")
        with col2:
            st.metric("Interval", params.get('interval', 'monthly').title())
        with col3:
            st.metric("Top Categories", params.get('limit', 'N/A'))
        with col4:
            st.metric("Total Series", len(data) if isinstance(data, list) else 1)
    else:
        st.error("No data available for this chart")

def create_product_trend_chart():
    """Chart 3: Product Category Chart"""
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    
    # Header with fullscreen button
    col1, col2 = st.columns([4, 1])
    with col1:
        st.subheader("🛍️ Product Category Comparison")
        st.markdown("""
        <div style="background-color: #f0f2f6; padding: 0.5rem; border-radius: 5px; margin-bottom: 1rem; font-size: 0.85rem;">
        <b>🛍️ How to Read:</b> Grouped bar chart showing revenue performance of top product categories over time. 
        Each color represents a different product category. Use to identify <b>bestselling products</b>, <b>category trends</b>, and <b>product lifecycle patterns</b>.
        </div>
        """, unsafe_allow_html=True)
    with col2:
        product_fullscreen_btn = st.button("🔍 Fullscreen", key="product_fullscreen", help="View in fullscreen popup")
    
    # Individual filters
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        product_start_date = st.date_input("Start Date", value=datetime(2025, 4, 1), key="product_start")
    with col2:
        product_end_date = st.date_input("End Date", value=datetime(2025, 6, 30), key="product_end")
    with col3:
        product_interval = st.selectbox("Grouping", ["Monthly", "Weekly", "Daily"], key="product_interval")
    with col4:
        product_limit = st.slider("Top N Categories", 5, 20, 10, key="product_limit")
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Fetch data
    params = {
        'start_date': product_start_date.isoformat() if product_start_date else None,
        'end_date': product_end_date.isoformat() if product_end_date else None,
        'interval': product_interval.lower(),
        'limit': product_limit
    }
    
    chart_data = fetch_chart_data('product-trend', params)
    
    # Show fullscreen in new tab if button clicked
    if product_fullscreen_btn:
        # Create URL with parameters
        base_url = "http://localhost:8501"
        params_str = "&".join([f"{k}={v}" for k, v in params.items() if v is not None])
        fullscreen_url = f"{base_url}?fullscreen=product_performance&{params_str}"
        
        # Show link button to open in new tab
        st.markdown(f"""
        <a href="{fullscreen_url}" target="_blank" style="
            display: inline-block;
            padding: 0.5rem 1rem;
            background-color: #ff7f0e;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            font-weight: bold;
            margin: 0.5rem 0;
        ">🔍 Open Fullscreen Chart in New Tab</a>
        """, unsafe_allow_html=True)
    
    if chart_data and chart_data.get('data'):
        data = chart_data['data']
        
        fig = go.Figure()
        
        # Handle multiple series (new API structure) - Use Bar Chart for better visibility
        if isinstance(data, list) and len(data) > 0:
            colors = ['#ff7f0e', '#1f77b4', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f']
            
            # Get all time periods from first series
            if data[0].get('x'):
                time_periods = data[0]['x']
                
                # Create grouped bar chart
                for i, series in enumerate(data):
                    color = colors[i % len(colors)]
                    fig.add_trace(go.Bar(
                        x=series.get('x', []),
                        y=series.get('y', []),
                        name=series.get('name', f'Product {i+1}'),
                        marker_color=color,
                        hovertemplate=f'<b>{series.get("name", "Product")}</b><br>Period: %{{x}}<br>Revenue: Rp %{{y:,.0f}}<extra></extra>'
                    ))
                    
                # Update layout for grouped bar chart
                fig.update_layout(
                    barmode='group',
                    bargap=0.15,
                    bargroupgap=0.1
                )
        else:
            # Fallback for old API structure
            fig.add_trace(go.Bar(
                x=data.get('x', []) if hasattr(data, 'get') else [],
                y=data.get('y', []) if hasattr(data, 'get') else [],
                name='Product Performance',
                marker_color='#ff7f0e'
            ))
        
        fig.update_layout(
            title=chart_data.get('title', 'Product Category Performance'),
            height=400,
            showlegend=True,
            hovermode='closest',
            xaxis_title="Category Index",
            yaxis_title="Revenue (Rp)"
        )
        
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': True, 'responsive': True})
    else:
        st.warning("No data available for product trends")
    
    st.markdown('</div>', unsafe_allow_html=True)

def create_fullscreen_payment_method(params):
    """Create fullscreen payment method chart"""
    st.title("💳 Payment Method Analysis - Full Screen")
    
    # Fetch data with same parameters
    chart_data = fetch_chart_data('payment-trend', params)
    
    if chart_data and chart_data.get('data'):
        data = chart_data['data']
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=data.get('x', []),
            y=data.get('y', []),
            mode='lines+markers',
            name=data.get('name', 'Payment Methods'),
            line=dict(color='#9467bd', width=4),
            marker=dict(size=12, color='#8c564b'),
            text=data.get('labels', []),
            hovertemplate='<b>%{text}</b><br>Sales: Rp %{y:,.0f}<extra></extra>',
            fill='tonexty'
        ))
        
        fig.update_layout(
            title=chart_data.get('title', 'Payment Method Trends Analysis'),
            height=700,
            showlegend=True,
            hovermode='closest',
            xaxis_title="Payment Method Index",
            yaxis_title="Sales (Rp)",
            font=dict(size=18)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Date Range", f"{params.get('start_date', 'N/A')} to {params.get('end_date', 'N/A')}")
        with col2:
            st.metric("Interval", params.get('interval', 'monthly').title())
        with col3:
            st.metric("Data Points", len(data.get('x', [])))
    else:
        st.error("No data available for this chart")

def create_payment_trend_chart():
    """Chart 4: Payment Method Trend Line Chart"""
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    
    # Header with fullscreen button
    col1, col2 = st.columns([4, 1])
    with col1:
        st.subheader("💳 Payment Method Trends")
        st.markdown("""
        <div style="background-color: #f0f2f6; padding: 0.5rem; border-radius: 5px; margin-bottom: 1rem; font-size: 0.85rem;">
        <b>💳 How to Read:</b> Line chart showing payment method adoption and usage trends over time. 
        Track changes in customer payment preferences. Use to analyze <b>digital payment adoption</b>, <b>cash usage trends</b>, and <b>payment method shifts</b>.
        </div>
        """, unsafe_allow_html=True)
    with col2:
        payment_fullscreen_btn = st.button("🔍 Fullscreen", key="payment_fullscreen", help="View in fullscreen popup")
    
    # Individual filters
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        payment_start_date = st.date_input("Start Date", value=datetime(2025, 4, 1), key="payment_start")
    with col2:
        payment_end_date = st.date_input("End Date", value=datetime(2025, 6, 30), key="payment_end")
    with col3:
        payment_interval = st.selectbox("Interval", ["Monthly", "Weekly", "Daily"], key="payment_interval")
    with col4:
        st.empty()  # Remove refresh button
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Fetch data
    params = {
        'start_date': payment_start_date.isoformat() if payment_start_date else None,
        'end_date': payment_end_date.isoformat() if payment_end_date else None,
        'interval': payment_interval.lower()
    }
    chart_data = fetch_chart_data('payment-trend', params)
    
    # Show fullscreen in new tab if button clicked
    if payment_fullscreen_btn:
        # Create URL with parameters
        base_url = "http://localhost:8501"
        params_str = "&".join([f"{k}={v}" for k, v in params.items() if v is not None])
        fullscreen_url = f"{base_url}?fullscreen=payment_method&{params_str}"
        
        # Show link button to open in new tab
        st.markdown(f"""
        <a href="{fullscreen_url}" target="_blank" style="
            display: inline-block;
            padding: 0.5rem 1rem;
            background-color: #9467bd;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            font-weight: bold;
            margin: 0.5rem 0;
        ">🔍 Open Fullscreen Chart in New Tab</a>
        """, unsafe_allow_html=True)
    
    if chart_data and chart_data.get('data'):
        data = chart_data['data']
        
        fig = go.Figure()
        
        # Handle multiple payment method series
        if isinstance(data, list):
            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
            for i, series in enumerate(data):
                fig.add_trace(go.Scatter(
                    x=series.get('x', []),
                    y=series.get('y', []),
                    mode='lines+markers',
                    name=series.get('name', f'Payment Method {i+1}'),
                    line=dict(color=colors[i % len(colors)], width=3),
                    marker=dict(size=8, color=colors[i % len(colors)]),
                    hovertemplate='<b>%{fullData.name}</b><br>Sales: Rp %{y:,.0f}<extra></extra>'
                ))
        else:
            # Fallback for single series data
            fig.add_trace(go.Scatter(
                x=data.get('x', []),
                y=data.get('y', []),
                mode='lines+markers',
                name=data.get('name', 'Payment Methods'),
                line=dict(color='#9467bd', width=3),
                marker=dict(size=10, color='#8c564b'),
                text=data.get('labels', []),
                hovertemplate='<b>%{text}</b><br>Sales: Rp %{y:,.0f}<extra></extra>'
            ))
        
        fig.update_layout(
            title=chart_data.get('title', 'Payment Method Trends'),
            height=400,
            showlegend=True,
            hovermode='closest',
            xaxis_title="Payment Method Index",
            yaxis_title="Sales (Rp)"
        )
        
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': True, 'responsive': True})
    else:
        st.warning("No data available for payment trends")
    
    st.markdown('</div>', unsafe_allow_html=True)

def create_fullscreen_candlestick(params):
    """Create fullscreen candlestick chart"""
    st.title("📈 Revenue Candlestick Analysis - Full Screen")
    
    # Fetch data with same parameters
    chart_data = fetch_chart_data('revenue-candlestick', params)
    
    if chart_data and chart_data.get('data'):
        data = chart_data['data']
        
        fig = go.Figure(data=go.Candlestick(
            x=[item['x'] for item in data],
            open=[item['open'] for item in data],
            high=[item['high'] for item in data],
            low=[item['low'] for item in data],
            close=[item['close'] for item in data],
            name="Revenue OHLC",
            increasing_line_color='#2ca02c',
            decreasing_line_color='#d62728'
        ))
        
        fig.update_layout(
            title=chart_data.get('title', 'Revenue Candlestick Analysis'),
            height=750,
            showlegend=True,
            xaxis_title="Time Period",
            yaxis_title="Revenue (Rp)",
            xaxis_rangeslider_visible=True,  # Enable range slider in fullscreen
            font=dict(size=18)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Date Range", f"{params.get('start_date', 'N/A')} to {params.get('end_date', 'N/A')}")
        with col2:
            st.metric("Interval", params.get('interval', 'monthly').title())
        with col3:
            st.metric("Data Points", len(data))
        with col4:
            avg_close = sum([item['close'] for item in data]) / len(data) if data else 0
            st.metric("Avg Close", f"Rp {avg_close:,.0f}")
    else:
        st.error("No data available for this chart")

def create_revenue_candlestick_chart():
    """Chart 5: Revenue Candlestick Chart"""
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    
    # Header with fullscreen button
    col1, col2 = st.columns([4, 1])
    with col1:
        st.subheader("📊 Revenue Candlestick Analysis")
        st.markdown("""
        <div style="background-color: #f0f2f6; padding: 0.5rem; border-radius: 5px; margin-bottom: 1rem; font-size: 0.85rem;">
        <b>📊 How to Read:</b> OHLC candlestick chart showing revenue volatility patterns (Open-High-Low-Close per period). 
        Green candles = revenue increased, Red candles = revenue decreased. Use to identify <b>revenue volatility</b>, <b>market patterns</b>, and <b>performance stability</b>.
        </div>
        """, unsafe_allow_html=True)
    with col2:
        candlestick_fullscreen_btn = st.button("🔍 Fullscreen", key="candlestick_fullscreen", help="View in fullscreen popup")
    
    # Individual filters
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        start_date = st.date_input("Start Date", value=datetime(2025, 4, 1), key="candle_start")
    with col2:
        end_date = st.date_input("End Date", value=datetime(2025, 6, 30), key="candle_end")
    with col3:
        candle_interval = st.selectbox("Interval", ["Monthly", "Weekly", "Daily"], key="candle_interval")
    with col4:
        st.empty()  # Remove refresh button
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Create unique cache key for filters
    cache_key = f"candlestick_{start_date}_{end_date}_{candle_interval}"
    
    # Fetch data
    params = {
        'start_date': start_date.isoformat() if start_date else None,
        'end_date': end_date.isoformat() if end_date else None,
        'interval': candle_interval.lower()
    }
    chart_data = fetch_chart_data('revenue-candlestick', params, cache_key)
    
    # Show fullscreen in new tab if button clicked
    if candlestick_fullscreen_btn:
        # Create URL with parameters
        base_url = "http://localhost:8501"
        params_str = "&".join([f"{k}={v}" for k, v in params.items() if v is not None])
        fullscreen_url = f"{base_url}?fullscreen=candlestick&{params_str}"
        
        # Show link button to open in new tab
        st.markdown(f"""
        <a href="{fullscreen_url}" target="_blank" style="
            display: inline-block;
            padding: 0.5rem 1rem;
            background-color: #d62728;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            font-weight: bold;
            margin: 0.5rem 0;
        ">🔍 Open Fullscreen Chart in New Tab</a>
        """, unsafe_allow_html=True)
    
    if chart_data and chart_data.get('data'):
        data = chart_data['data']
        
        fig = go.Figure(data=go.Candlestick(
            x=[item['x'] for item in data],
            open=[item['open'] for item in data],
            high=[item['high'] for item in data],
            low=[item['low'] for item in data],
            close=[item['close'] for item in data],
            name="Revenue OHLC",
            increasing_line_color='#2ca02c',
            decreasing_line_color='#d62728'
        ))
        
        fig.update_layout(
            title=chart_data.get('title', f'Revenue Candlestick - {start_date} to {end_date}'),
            height=400,
            showlegend=False,
            xaxis_title="Time Period",
            yaxis_title="Revenue (Rp)",
            xaxis_rangeslider_visible=False
        )
        
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': True, 'responsive': True})
        
            
    else:
        st.warning("No data available for revenue candlestick")
        
    
    st.markdown('</div>', unsafe_allow_html=True)

def create_fullscreen_transaction_volume(params):
    """Create fullscreen transaction volume chart"""
    st.title("🧾 Transaction Volume Analysis - Full Screen")
    
    # Fetch data with same parameters
    chart_data = fetch_chart_data('transaction-volume', params)
    
    if chart_data and chart_data.get('data'):
        data = chart_data['data']
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=data.get('x', []),
            y=data.get('y', []),
            mode='lines+markers',
            name=data.get('name', 'Transaction Volume'),
            line=dict(color='#17becf', width=4),
            marker=dict(size=10, color='#bcbd22'),
            fill='tonexty',
            fillcolor='rgba(23, 190, 207, 0.3)'
        ))
        
        # Calculate Y-axis range to show differences clearly
        y_values = data.get('y', [])
        if y_values:
            min_y = min(y_values)
            max_y = max(y_values)
            
            # Add padding to make differences more visible
            y_range = max_y - min_y
            padding = y_range * 0.1 if y_range > 0 else max_y * 0.1
            
            yaxis_config = dict(
                title="Number of Transactions",
                range=[max(0, min_y - padding), max_y + padding],
                autorange=False
            )
        else:
            yaxis_config = dict(title="Number of Transactions")
        
        fig.update_layout(
            title=chart_data.get('title', 'Transaction Volume Analysis Over Time'),
            height=700,
            showlegend=True,
            hovermode='x unified',
            xaxis_title="Time Period",
            yaxis=yaxis_config,
            font=dict(size=18)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Date Range", f"{params.get('start_date', 'N/A')} to {params.get('end_date', 'N/A')}")
        with col2:
            st.metric("Interval", params.get('interval', 'monthly').title())
        with col3:
            st.metric("Data Points", len(data.get('x', [])))
    else:
        st.error("No data available for this chart")

def create_transaction_volume_chart():
    """Chart 6: Transaction Volume Line Chart"""
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    
    # Header with fullscreen button
    col1, col2 = st.columns([4, 1])
    with col1:
        st.subheader("🧾 Transaction Volume Trend")
        st.markdown("""
        <div style="background-color: #f0f2f6; padding: 0.5rem; border-radius: 5px; margin-bottom: 1rem; font-size: 0.85rem;">
        <b>🧾 How to Read:</b> Area chart showing the number of transactions over time periods. 
        Higher areas indicate more customer activity. Use to analyze <b>customer traffic patterns</b>, <b>business activity levels</b>, and <b>operational capacity</b>.
        </div>
        """, unsafe_allow_html=True)
    with col2:
        volume_fullscreen_btn = st.button("🔍 Fullscreen", key="volume_fullscreen", help="View in fullscreen popup")
    
    # Individual filters
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        volume_start_date = st.date_input("Start Date", value=datetime(2025, 4, 1), key="volume_start")
    with col2:
        volume_end_date = st.date_input("End Date", value=datetime(2025, 6, 30), key="volume_end")
    with col3:
        volume_interval = st.selectbox("Interval", ["Monthly", "Weekly", "Daily"], key="volume_interval")
    with col4:
        st.empty()  # Remove refresh button
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Fetch data
    params = {
        'start_date': volume_start_date.isoformat() if volume_start_date else None,
        'end_date': volume_end_date.isoformat() if volume_end_date else None,
        'interval': volume_interval.lower()
    }
    chart_data = fetch_chart_data('transaction-volume', params)
    
    # Show fullscreen in new tab if button clicked
    if volume_fullscreen_btn:
        # Create URL with parameters
        base_url = "http://localhost:8501"
        params_str = "&".join([f"{k}={v}" for k, v in params.items() if v is not None])
        fullscreen_url = f"{base_url}?fullscreen=transaction_volume&{params_str}"
        
        # Show link button to open in new tab
        st.markdown(f"""
        <a href="{fullscreen_url}" target="_blank" style="
            display: inline-block;
            padding: 0.5rem 1rem;
            background-color: #17becf;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            font-weight: bold;
            margin: 0.5rem 0;
        ">🔍 Open Fullscreen Chart in New Tab</a>
        """, unsafe_allow_html=True)
    
    if chart_data and chart_data.get('data'):
        data = chart_data['data']
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=data.get('x', []),
            y=data.get('y', []),
            mode='lines+markers',
            name=data.get('name', 'Transaction Volume'),
            line=dict(color='#17becf', width=3),
            marker=dict(size=8, color='#bcbd22'),
            fill='tonexty' if data.get('fill') else None,
            fillcolor='rgba(23, 190, 207, 0.3)'
        ))
        
        # Calculate Y-axis range to show differences clearly
        y_values = data.get('y', [])
        if y_values:
            min_y = min(y_values)
            max_y = max(y_values)
            
            # Add padding to make differences more visible
            y_range = max_y - min_y
            padding = y_range * 0.1 if y_range > 0 else max_y * 0.1
            
            yaxis_config = dict(
                title="Number of Transactions",
                range=[max(0, min_y - padding), max_y + padding],
                autorange=False
            )
        else:
            yaxis_config = dict(title="Number of Transactions")
        
        fig.update_layout(
            title=chart_data.get('title', 'Transaction Volume Over Time'),
            height=400,
            showlegend=True,
            hovermode='x unified',
            xaxis_title="Time Period",
            yaxis=yaxis_config
        )
        
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': True, 'responsive': True})
    else:
        st.warning("No data available for transaction volume")
    
    st.markdown('</div>', unsafe_allow_html=True)

def main():
    """Main dashboard function"""
    
    # Check if this is a fullscreen view
    query_params = st.query_params
    fullscreen_chart = query_params.get('fullscreen', None)
    
    if fullscreen_chart:
        # Apply fullscreen CSS
        st.markdown("""
        <style>
        .main .block-container {
            padding-top: 1rem;
            padding-bottom: 1rem;
            max-width: 100%;
        }
        </style>
        """, unsafe_allow_html=True)
        
        
    if fullscreen_chart:
        if fullscreen_chart == 'product_time_analysis':
            params = {
                'start_date': query_params.get('start_date'),
                'end_date': query_params.get('end_date'),
                'interval': query_params.get('interval', 'monthly'),
                'limit': int(query_params.get('limit', 10))
            }
            create_fullscreen_product_time_analysis(params)
            return
        elif fullscreen_chart == 'sales_trend':
            params = {
                'start_date': query_params.get('start_date'),
                'end_date': query_params.get('end_date'),
                'interval': query_params.get('interval', 'monthly'),
                'locations': [query_params.get('locations')] if query_params.get('locations') and query_params.get('locations') != 'All' else None
            }
            create_fullscreen_sales_trend(params)
            return
        elif fullscreen_chart == 'location_performance':
            params = {
                'start_date': query_params.get('start_date'),
                'end_date': query_params.get('end_date'),
                'interval': query_params.get('interval', 'monthly'),
                'limit': int(query_params.get('limit', 15))
            }
            create_fullscreen_location_performance(params)
            return
        elif fullscreen_chart == 'product_performance':
            params = {
                'start_date': query_params.get('start_date'),
                'end_date': query_params.get('end_date'),
                'interval': query_params.get('interval', 'monthly'),
                'limit': int(query_params.get('limit', 10))
            }
            create_fullscreen_product_performance(params)
            return
        elif fullscreen_chart == 'payment_method':
            params = {
                'start_date': query_params.get('start_date'),
                'end_date': query_params.get('end_date'),
                'interval': query_params.get('interval', 'monthly')
            }
            create_fullscreen_payment_method(params)
            return
        elif fullscreen_chart == 'candlestick':
            params = {
                'start_date': query_params.get('start_date'),
                'end_date': query_params.get('end_date'),
                'interval': query_params.get('interval', 'monthly')
            }
            create_fullscreen_candlestick(params)
            return
        elif fullscreen_chart == 'transaction_volume':
            params = {
                'start_date': query_params.get('start_date'),
                'end_date': query_params.get('end_date'),
                'interval': query_params.get('interval', 'monthly')
            }
            create_fullscreen_transaction_volume(params)
            return
        else:
            st.error(f"Unknown fullscreen chart: {fullscreen_chart}")
            return
    
    # Header
    st.title("📈 Tea Shop Analytics Dashboard")
    
    # KPI Summary Row
    st.markdown('<div class="metric-row">', unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Sales", "Rp 762.4B", "↑12.5%")
    with col2:
        st.metric("Active Locations", "705", "↑3")
    with col3:
        st.metric("Avg Transaction", "Rp 209K", "↑5.2%")
    with col4:
        st.metric("Total Transactions", "3.64M", "↑8.1%")
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Top Analysis Chart - Full Width
    create_product_time_analysis_chart()
    
    # Separator
    st.markdown("---")
    st.markdown("### **Individual Chart Analysis**")
    
    # Row 1: Sales Trend and Location Performance
    col1, col2 = st.columns(2)
    with col1:
        create_sales_trend_chart()
    with col2:
        create_location_performance_chart()
    
    # Row 2: Product Trends and Payment Trends
    col1, col2 = st.columns(2)
    with col1:
        create_product_trend_chart()
    with col2:
        create_payment_trend_chart()
    
    # Row 3: Candlestick and Transaction Volume
    col1, col2 = st.columns(2)
    with col1:
        create_revenue_candlestick_chart()
    with col2:
        create_transaction_volume_chart()
    

if __name__ == "__main__":
    main()