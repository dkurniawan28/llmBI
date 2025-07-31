#!/usr/bin/env python3
"""
Chart Description Helper Functions
Provides descriptions for each chart to help analysts read them properly
"""

import streamlit as st

def show_chart_description(chart_type):
    """Show description for specific chart type"""
    
    descriptions = {
        'product_time_analysis': {
            'title': '📊 Chart Description:',
            'text': '''This stacked bar chart shows the sales performance of top products over time. 
            Each bar represents a time period, with different colored segments showing individual product contributions. 
            Use this to identify <b>trending products</b>, <b>seasonal patterns</b>, and <b>product mix changes</b> over time.''',
            'bg_color': '#f0f8ff'
        },
        
        'sales_trend': {
            'title': '📈 How to Read:',
            'text': '''Line chart showing sales trends across time periods. 
            Filter by specific location to analyze branch performance. Use to identify <b>growth patterns</b>, <b>seasonal trends</b>, and <b>performance dips</b>.''',
            'bg_color': '#f0f2f6'
        },
        
        'location_performance': {
            'title': '🏪 How to Read:',
            'text': '''Grouped bar chart comparing sales performance across top locations by time period. 
            Each color represents a different location. Use to identify <b>best performing branches</b>, <b>regional trends</b>, and <b>location-specific seasonality</b>.''',
            'bg_color': '#f0f2f6'
        },
        
        'product_category': {
            'title': '🛍️ How to Read:',
            'text': '''Grouped bar chart showing revenue performance of top product categories over time. 
            Each color represents a different product category. Use to identify <b>bestselling products</b>, <b>category trends</b>, and <b>product lifecycle patterns</b>.''',
            'bg_color': '#f0f2f6'
        },
        
        'payment_method': {
            'title': '💳 How to Read:',
            'text': '''Line chart showing payment method adoption and usage trends over time. 
            Track changes in customer payment preferences. Use to analyze <b>digital payment adoption</b>, <b>cash usage trends</b>, and <b>payment method shifts</b>.''',
            'bg_color': '#f0f2f6'
        },
        
        'candlestick': {
            'title': '📊 How to Read:',
            'text': '''OHLC candlestick chart showing revenue volatility patterns (Open-High-Low-Close per period). 
            Green candles = revenue increased, Red candles = revenue decreased. Use to identify <b>revenue volatility</b>, <b>market patterns</b>, and <b>performance stability</b>.''',
            'bg_color': '#f0f2f6'
        },
        
        'transaction_volume': {
            'title': '🧾 How to Read:',
            'text': '''Area chart showing the number of transactions over time periods. 
            Higher areas indicate more customer activity. Use to analyze <b>customer traffic patterns</b>, <b>business activity levels</b>, and <b>operational capacity</b>.''',
            'bg_color': '#f0f2f6'
        }
    }
    
    if chart_type in descriptions:
        desc = descriptions[chart_type]
        st.markdown(f"""
        <div style="background-color: {desc['bg_color']}; padding: 0.5rem; border-radius: 5px; margin-bottom: 1rem; font-size: 0.85rem;">
        <b>{desc['title']}</b> {desc['text']}
        </div>
        """, unsafe_allow_html=True)

def get_analysis_insights(chart_type):
    """Get specific insights for analysts"""
    
    insights = {
        'product_time_analysis': [
            "🔍 Look for products with consistent growth across periods",
            "📈 Identify seasonal products that peak in specific months", 
            "⚠️ Watch for declining product segments that need attention",
            "🎯 Use stack heights to compare total market performance"
        ],
        
        'sales_trend': [
            "📊 Compare performance across different time intervals",
            "🏪 Filter by location to identify branch-specific patterns",
            "📈 Look for consistent growth trends or concerning dips",
            "🎯 Use trend direction to forecast future performance"
        ],
        
        'location_performance': [
            "🏆 Identify top-performing branches for best practices",
            "📍 Compare regional performance patterns",
            "⚠️ Flag underperforming locations needing support",
            "📈 Track location-specific seasonal trends"
        ],
        
        'product_category': [
            "🛍️ Identify bestselling product categories",
            "📊 Compare category performance over time",
            "🎯 Spot emerging or declining product trends",
            "💡 Use for inventory and marketing planning"
        ],
        
        'payment_method': [
            "💳 Track digital payment adoption rates",
            "💰 Monitor cash usage trends",
            "📱 Identify preferred payment methods",
            "🎯 Plan payment infrastructure investments"
        ],
        
        'candlestick': [
            "📊 Green candles indicate revenue growth periods",
            "📉 Red candles show revenue decline periods", 
            "📈 Long wicks indicate high volatility",
            "🎯 Use patterns to predict future performance"
        ],
        
        'transaction_volume': [
            "👥 Higher volume = more customer activity",
            "📅 Identify peak business hours/days",
            "📊 Compare activity levels across periods",
            "🎯 Plan staffing and operational capacity"
        ]
    }
    
    return insights.get(chart_type, [])

def show_analysis_tips(chart_type):
    """Show analysis tips for specific chart"""
    insights = get_analysis_insights(chart_type)
    
    if insights:
        st.markdown("**💡 Analysis Tips:**")
        for tip in insights:
            st.markdown(f"- {tip}")