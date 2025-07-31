#!/usr/bin/env python3
"""
Analytics Dashboard - Tea Shop Business Intelligence
Clean dashboard interface without chat functionality
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

# Page configuration
st.set_page_config(
    page_title="Tea Shop Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional dashboard
st.markdown("""
<style>
    .main > div {
        padding-top: 2rem;
    }
    
    .stMetric {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    
    .metric-container {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        margin: 0.5rem 0;
    }
    
    .filter-container {
        background-color: #f1f3f4;
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1rem;
    }
    
    .chart-container {
        background-color: white;
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }
    
    h1 {
        color: #1f77b4;
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }
    
    h2 {
        color: #2c3e50;
        font-size: 1.8rem;
        font-weight: 600;
        margin-top: 2rem;
        margin-bottom: 1rem;
    }
    
    h3 {
        color: #34495e;
        font-size: 1.4rem;
        font-weight: 500;
        margin-bottom: 0.8rem;
    }
    
    .sidebar .sidebar-content {
        background-color: #2c3e50;
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
API_BASE_URL = f"http://localhost:{os.getenv('API_PORT', '5002')}"

@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_data(endpoint_query):
    """Fetch data from API with caching"""
    try:
        response = requests.post(
            f"{API_BASE_URL}/aggregate/execute",
            json={"command": endpoint_query},
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success') and data.get('results'):
                return data['results']
        return []
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return []

@st.cache_data(ttl=600)  # Cache for 10 minutes
def get_filter_options():
    """Get available filter options"""
    try:
        # Get locations
        locations_data = fetch_data("show all locations")
        locations = []
        if locations_data:
            locations = [item.get('location_name', item.get('_id', '')) for item in locations_data]
        
        # Get product categories
        categories_data = fetch_data("show all product categories")
        categories = []
        if categories_data:
            categories = [item.get('product_category', item.get('_id', '')) for item in categories_data]
        
        # Get months (static for now)
        months = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        
        return {
            'locations': sorted(locations) if locations else ["All Locations"],
            'categories': sorted(categories) if categories else ["All Categories"], 
            'months': months
        }
    except Exception as e:
        st.error(f"Error loading filter options: {e}")
        return {'locations': ["All"], 'categories': ["All"], 'months': ["All"]}

def create_kpi_metrics():
    """Create KPI metrics row"""
    
    # Fetch overall metrics
    total_sales_data = fetch_data("show total sales")
    total_locations_data = fetch_data("show number of locations")
    avg_transaction_data = fetch_data("show average transaction value")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_sales = 0
        if total_sales_data:
            total_sales = total_sales_data[0].get('total_sales', 0)
        
        st.markdown(f"""
        <div class="metric-container">
            <h3 style="margin: 0; color: white;">💰 Total Sales</h3>
            <h2 style="margin: 0; color: white;">Rp {total_sales:,.0f}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        total_locations = len(total_locations_data) if total_locations_data else 0
        st.markdown(f"""
        <div class="metric-container">
            <h3 style="margin: 0; color: white;">🏪 Active Locations</h3>
            <h2 style="margin: 0; color: white;">{total_locations}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        avg_transaction = 0
        if avg_transaction_data:
            avg_transaction = avg_transaction_data[0].get('average_transaction', 0)
        
        st.markdown(f"""
        <div class="metric-container">
            <h3 style="margin: 0; color: white;">📊 Avg Transaction</h3>
            <h2 style="margin: 0; color: white;">Rp {avg_transaction:,.0f}</h2>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        # Calculate total transactions
        total_transactions = 0
        if total_sales_data and avg_transaction > 0:
            total_transactions = total_sales / avg_transaction
        
        st.markdown(f"""
        <div class="metric-container">
            <h3 style="margin: 0; color: white;">🧾 Total Transactions</h3>
            <h2 style="margin: 0; color: white;">{total_transactions:,.0f}</h2>
        </div>
        """, unsafe_allow_html=True)

def create_sales_overview_chart(selected_period="All"):
    """Chart 1: Sales Performance Overview"""
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    st.subheader("📈 Sales Performance Overview")
    
    # Fetch monthly sales data
    monthly_data = fetch_data("show sales by month")
    
    if monthly_data:
        df = pd.DataFrame(monthly_data)
        
        # Create subplot with secondary y-axis
        fig = make_subplots(
            rows=1, cols=1,
            secondary_y=True,
            subplot_titles=["Monthly Sales Performance"]
        )
        
        # Sales line
        fig.add_trace(
            go.Scatter(
                x=df.get('month_name', df.get('month', [])),
                y=df.get('total_sales', []),
                mode='lines+markers',
                name='Sales (Rp)',
                line=dict(color='#1f77b4', width=3),
                marker=dict(size=8)
            ),
            secondary_y=False
        )
        
        # Transactions bar
        fig.add_trace(
            go.Bar(
                x=df.get('month_name', df.get('month', [])),
                y=df.get('total_transactions', []),
                name='Transactions',
                marker=dict(color='rgba(31, 119, 180, 0.3)'),
                yaxis='y2'
            ),
            secondary_y=True
        )
        
        fig.update_layout(
            height=400,
            showlegend=True,
            title_font_size=16,
            hovermode='x unified'
        )
        
        fig.update_yaxes(title_text="Sales Amount (Rp)", secondary_y=False)
        fig.update_yaxes(title_text="Number of Transactions", secondary_y=True)
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data available for sales overview")
    
    st.markdown('</div>', unsafe_allow_html=True)

def create_location_performance_chart(selected_locations=None):
    """Chart 2: Location Performance Comparison"""
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    st.subheader("🏪 Location Performance Comparison")
    
    # Fetch location performance data
    location_data = fetch_data("show sales by location")
    
    if location_data:
        df = pd.DataFrame(location_data)
        
        # Filter by selected locations if provided
        if selected_locations and "All Locations" not in selected_locations:
            df = df[df['location_name'].isin(selected_locations)]
        
        # Sort by total sales and get top 15
        df = df.sort_values('total_sales', ascending=False).head(15)
        
        # Create horizontal bar chart
        fig = px.bar(
            df,
            x='total_sales',
            y='location_name',
            orientation='h',
            title="Top 15 Locations by Sales",
            labels={'total_sales': 'Sales (Rp)', 'location_name': 'Location'},
            color='total_sales',
            color_continuous_scale='Blues'
        )
        
        fig.update_layout(
            height=500,
            yaxis={'categoryorder': 'total ascending'},
            showlegend=False
        )
        
        fig.update_traces(
            texttemplate='%{x:,.0f}',
            textposition='outside'
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show summary stats
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Top Performer", df.iloc[0]['location_name'])
        with col2:
            st.metric("Highest Sales", f"Rp {df.iloc[0]['total_sales']:,.0f}")
        with col3:
            st.metric("Average Sales", f"Rp {df['total_sales'].mean():,.0f}")
    else:
        st.warning("No data available for location performance")
    
    st.markdown('</div>', unsafe_allow_html=True)

def create_product_category_chart(selected_categories=None):
    """Chart 3: Product Category Analysis"""
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    st.subheader("🛍️ Product Category Analysis")
    
    # Fetch product category data
    category_data = fetch_data("show sales by product category")
    
    if category_data:
        df = pd.DataFrame(category_data)
        
        # Filter by selected categories if provided
        if selected_categories and "All Categories" not in selected_categories:
            df = df[df['product_category'].isin(selected_categories)]
        
        # Create pie chart and bar chart side by side
        col1, col2 = st.columns(2)
        
        with col1:
            fig_pie = px.pie(
                df.head(10),
                values='total_revenue',
                names='product_category',
                title="Category Distribution (Top 10)"
            )
            fig_pie.update_traces(textposition='inside', textinfo='percent+label')
            fig_pie.update_layout(height=400)
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            fig_bar = px.bar(
                df.head(10),
                x='product_category',
                y='total_revenue',
                title="Category Revenue (Top 10)",
                color='total_revenue',
                color_continuous_scale='Viridis'
            )
            fig_bar.update_layout(
                height=400,
                xaxis_tickangle=45,
                showlegend=False
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        
        # Summary table
        st.subheader("Category Performance Summary")
        summary_df = df.head(10)[['product_category', 'total_revenue', 'total_quantity']].copy()
        summary_df['total_revenue'] = summary_df['total_revenue'].apply(lambda x: f"Rp {x:,.0f}")
        summary_df['total_quantity'] = summary_df['total_quantity'].apply(lambda x: f"{x:,.0f}")
        summary_df.columns = ['Category', 'Revenue', 'Quantity Sold']
        st.dataframe(summary_df, use_container_width=True)
    else:
        st.warning("No data available for product categories")
    
    st.markdown('</div>', unsafe_allow_html=True)

def create_monthly_trend_chart(selected_year=2024):
    """Chart 4: Monthly Sales Trend"""
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    st.subheader("📅 Monthly Sales Trend")
    
    # Fetch monthly trend data
    monthly_data = fetch_data(f"show monthly sales trend for {selected_year}")
    
    if monthly_data:
        df = pd.DataFrame(monthly_data)
        
        # Create area chart for trend
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=df.get('month_name', df.get('month', [])),
            y=df.get('total_sales', []),
            mode='lines+markers',
            fill='tonexty',
            name='Sales Trend',
            line=dict(color='#2E86AB', width=3),
            marker=dict(size=8, color='#A23B72')
        ))
        
        # Add trend line
        if len(df) > 1:
            z = np.polyfit(range(len(df)), df.get('total_sales', []), 1)
            p = np.poly1d(z)
            fig.add_trace(go.Scatter(
                x=df.get('month_name', df.get('month', [])),
                y=p(range(len(df))),
                mode='lines',
                name='Trend Line',
                line=dict(color='red', dash='dash', width=2)
            ))
        
        fig.update_layout(
            title=f"Sales Trend Analysis - {selected_year}",
            height=400,
            hovermode='x unified',
            showlegend=True
        )
        
        fig.update_xaxes(title_text="Month")
        fig.update_yaxes(title_text="Sales (Rp)")
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Growth analysis
        if len(df) > 1:
            total_growth = ((df.iloc[-1]['total_sales'] - df.iloc[0]['total_sales']) / df.iloc[0]['total_sales']) * 100
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Period Growth", f"{total_growth:.1f}%")
            with col2:
                st.metric("Peak Month", df.loc[df['total_sales'].idxmax(), 'month_name'])
            with col3:
                st.metric("Peak Sales", f"Rp {df['total_sales'].max():,.0f}")
    else:
        st.warning("No data available for monthly trends")
    
    st.markdown('</div>', unsafe_allow_html=True)

def create_payment_method_chart():
    """Chart 5: Payment Method Distribution"""
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    st.subheader("💳 Payment Method Distribution")
    
    # Fetch payment method data
    payment_data = fetch_data("show payment method analysis")
    
    if payment_data:
        df = pd.DataFrame(payment_data)
        
        # Create donut chart
        fig = px.pie(
            df,
            values='total_sales',
            names='payment_method',
            title="Payment Method Distribution by Sales Value",
            hole=0.4
        )
        
        fig.update_traces(
            textposition='inside',
            textinfo='percent+label',
            textfont_size=12
        )
        
        fig.update_layout(
            height=400,
            annotations=[dict(text='Payment<br>Methods', x=0.5, y=0.5, font_size=16, showarrow=False)]
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Payment method summary table
            st.subheader("Payment Summary")
            summary_df = df.copy()
            summary_df['percentage'] = (summary_df['total_sales'] / summary_df['total_sales'].sum() * 100).round(1)
            summary_df['total_sales'] = summary_df['total_sales'].apply(lambda x: f"Rp {x:,.0f}")
            summary_df['percentage'] = summary_df['percentage'].apply(lambda x: f"{x}%")
            summary_df.columns = ['Payment Method', 'Total Sales', 'Percentage']
            st.dataframe(summary_df, use_container_width=True)
    else:
        st.warning("No data available for payment methods")
    
    st.markdown('</div>', unsafe_allow_html=True)

def create_top_products_chart(selected_categories=None, limit=20):
    """Chart 6: Top Products Performance"""
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    st.subheader("🏆 Top Products Performance")
    
    # Fetch top products data
    products_data = fetch_data(f"show top {limit} products by revenue")
    
    if products_data:
        df = pd.DataFrame(products_data)
        
        # Filter by category if selected
        if selected_categories and "All Categories" not in selected_categories:
            df = df[df['product_category'].isin(selected_categories)]
        
        df = df.head(limit)
        
        # Create bubble chart
        fig = px.scatter(
            df,
            x='total_quantity',
            y='total_revenue',
            size='total_revenue',
            color='product_category',
            hover_name='product_name',
            title=f"Top {limit} Products: Revenue vs Quantity",
            labels={
                'total_quantity': 'Quantity Sold',
                'total_revenue': 'Revenue (Rp)',
                'product_category': 'Category'
            }
        )
        
        fig.update_layout(height=500)
        fig.update_traces(marker=dict(sizemode='diameter', sizeref=df['total_revenue'].max()/1000))
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Top products table
        st.subheader("Top Products Detail")
        display_df = df[['product_name', 'product_category', 'total_revenue', 'total_quantity']].copy()
        display_df['total_revenue'] = display_df['total_revenue'].apply(lambda x: f"Rp {x:,.0f}")
        display_df['total_quantity'] = display_df['total_quantity'].apply(lambda x: f"{x:,.0f}")
        display_df.columns = ['Product Name', 'Category', 'Revenue', 'Quantity']
        st.dataframe(display_df, use_container_width=True)
    else:
        st.warning("No data available for top products")
    
    st.markdown('</div>', unsafe_allow_html=True)

def main():
    """Main dashboard function"""
    
    # Header
    st.title("☕ Tea Shop Analytics Dashboard")
    st.markdown("**Real-time Business Intelligence & Performance Monitoring**")
    st.markdown("---")
    
    # Sidebar filters
    st.sidebar.header("🔧 Dashboard Filters")
    
    # Load filter options
    filter_options = get_filter_options()
    
    # Location filter
    selected_locations = st.sidebar.multiselect(
        "📍 Select Locations",
        options=["All Locations"] + filter_options['locations'],
        default=["All Locations"]
    )
    
    # Category filter  
    selected_categories = st.sidebar.multiselect(
        "🛍️ Select Categories",
        options=["All Categories"] + filter_options['categories'],
        default=["All Categories"]
    )
    
    # Time period filter
    selected_year = st.sidebar.selectbox(
        "📅 Select Year",
        options=[2024, 2023, 2022],
        index=0
    )
    
    # Product limit filter
    product_limit = st.sidebar.slider(
        "🏆 Top Products Limit",
        min_value=10,
        max_value=50,
        value=20,
        step=5
    )
    
    # Refresh data button
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    # Display last update time
    st.sidebar.markdown("---")
    st.sidebar.info(f"📊 Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Main dashboard content
    with st.container():
        # KPI Metrics
        st.header("📊 Key Performance Indicators")
        create_kpi_metrics()
        
        st.markdown("---")
        
        # Charts Grid
        # Row 1: Sales Overview and Location Performance
        col1, col2 = st.columns(2)
        with col1:
            create_sales_overview_chart()
        with col2:
            create_location_performance_chart(selected_locations)
        
        # Row 2: Product Category and Monthly Trend
        col1, col2 = st.columns(2)
        with col1:
            create_product_category_chart(selected_categories)
        with col2:
            create_monthly_trend_chart(selected_year)
        
        # Row 3: Payment Methods and Top Products
        col1, col2 = st.columns(2)
        with col1:
            create_payment_method_chart()
        with col2:
            create_top_products_chart(selected_categories, product_limit)
    
    # Footer
    st.markdown("---")
    st.markdown("*Dashboard powered by MongoDB Analytics & Streamlit*")

if __name__ == "__main__":
    main()