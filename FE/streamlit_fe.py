import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import requests
import json
import os

# Set page config
st.set_page_config(
    page_title="Dashboard Analisis Penjualan",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for dark theme and larger fonts
st.markdown("""
<style>
    /* Dark theme and font customization */
    .stApp {
        background-color: #0e1117;
        color: #fafafa;
    }
    
    /* Larger fonts for all text elements */
    .stMarkdown, .stText, p, div {
        font-size: 16px !important;
        color: #fafafa !important;
    }
    
    /* Headers with larger fonts */
    h1 {
        font-size: 3rem !important;
        color: #00d4ff !important;
        text-shadow: 0 0 10px rgba(0, 212, 255, 0.3);
    }
    
    h2 {
        font-size: 2.2rem !important;
        color: #ff6b6b !important;
        text-shadow: 0 0 8px rgba(255, 107, 107, 0.3);
    }
    
    h3 {
        font-size: 1.8rem !important;
        color: #4ecdc4 !important;
        text-shadow: 0 0 6px rgba(78, 205, 196, 0.3);
    }
    
    /* Input fields styling */
    .stTextInput > div > div > input {
        background-color: #262730 !important;
        color: #fafafa !important;
        border: 2px solid #4ecdc4 !important;
        border-radius: 10px !important;
        font-size: 16px !important;
        padding: 12px !important;
    }
    
    .stTextInput > div > div > input:focus {
        border-color: #00d4ff !important;
        box-shadow: 0 0 10px rgba(0, 212, 255, 0.5) !important;
    }
    
    /* Button styling */
    .stButton > button {
        background: linear-gradient(45deg, #ff6b6b, #4ecdc4) !important;
        color: white !important;
        border: none !important;
        border-radius: 15px !important;
        font-size: 18px !important;
        font-weight: bold !important;
        padding: 12px 24px !important;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3) !important;
        transition: all 0.3s ease !important;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.4) !important;
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background-color: #1e1e2e !important;
    }
    
    .sidebar .sidebar-content {
        background-color: #1e1e2e !important;
        color: #fafafa !important;
    }
    
    /* Sidebar buttons */
    .stSidebar .stButton > button {
        background: linear-gradient(45deg, #667eea, #764ba2) !important;
        font-size: 16px !important;
        margin: 5px 0 !important;
    }
    
    /* Metrics styling */
    .metric-container {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%) !important;
        padding: 20px !important;
        border-radius: 15px !important;
        margin: 10px 0 !important;
        box-shadow: 0 8px 25px rgba(0, 0, 0, 0.3) !important;
    }
    
    .metric-container .metric-value {
        font-size: 2.5rem !important;
        font-weight: bold !important;
        color: white !important;
    }
    
    .metric-container .metric-label {
        font-size: 1.2rem !important;
        color: #e0e6ed !important;
    }
    
    /* Cards and containers */
    .stContainer, div[data-testid="stExpander"] {
        background-color: #262730 !important;
        border: 1px solid #4ecdc4 !important;
        border-radius: 15px !important;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2) !important;
        margin: 10px 0 !important;
    }
    
    /* DataFrames */
    .stDataFrame {
        background-color: #262730 !important;
        border-radius: 10px !important;
        overflow: hidden !important;
    }
    
    /* JSON display */
    .stJson {
        background-color: #1a1a2e !important;
        border: 1px solid #4ecdc4 !important;
        border-radius: 10px !important;
        font-size: 14px !important;
    }
    
    /* Alert boxes */
    .stAlert {
        font-size: 16px !important;
        border-radius: 10px !important;
    }
    
    /* Success messages */
    .stSuccess {
        background-color: rgba(76, 175, 80, 0.1) !important;
        border: 1px solid #4caf50 !important;
        color: #4caf50 !important;
    }
    
    /* Error messages */
    .stError {
        background-color: rgba(244, 67, 54, 0.1) !important;
        border: 1px solid #f44336 !important;
        color: #f44336 !important;
    }
    
    /* Warning messages */
    .stWarning {
        background-color: rgba(255, 152, 0, 0.1) !important;
        border: 1px solid #ff9800 !important;
        color: #ff9800 !important;
    }
    
    /* Spinner */
    .stSpinner {
        color: #00d4ff !important;
    }
    
    /* Custom info box styling */
    .info-box {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 25px;
        border-radius: 15px;
        border-left: 6px solid #00d4ff;
        box-shadow: 0 8px 25px rgba(0, 0, 0, 0.3);
        margin: 20px 0;
        color: white;
        font-size: 16px;
        line-height: 1.6;
    }
    
    /* Analytics description box */
    .analytics-box {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        padding: 25px;
        border-radius: 15px;
        border-left: 6px solid #4ecdc4;
        box-shadow: 0 8px 25px rgba(0, 0, 0, 0.4);
        margin: 20px 0;
        color: #fafafa;
        font-size: 17px;
        line-height: 1.7;
    }
    
    /* Plot background */
    .js-plotly-plot .plotly .modebar {
        background: rgba(38, 39, 48, 0.8) !important;
    }
</style>
""", unsafe_allow_html=True)

# API Configuration
API_BASE_URL = "http://localhost:5002"
OPENROUTER_API_KEY = os.getenv('OPENROUTER_API_KEY') or "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"

def prepare_chart_data(data):
    """Convert nested data structures to chart-friendly format"""
    if not data:
        return pd.DataFrame()
    
    chart_data = []
    
    for item in data:
        # Check if this is the product categories by location structure
        if 'location' in item and 'top_categories' in item:
            location = item['location']
            categories = item.get('top_categories', [])
            
            # Extract each category as a separate row for charting
            for category_info in categories[:10]:  # Limit to top 10 for readability
                if isinstance(category_info, dict) and 'category' in category_info and 'sales' in category_info:
                    chart_data.append({
                        'location': location,
                        'category': category_info['category'],
                        'sales': float(category_info['sales']) if category_info['sales'] else 0
                    })
        else:
            # For simple data structures, just flatten any nested objects
            flattened_item = {}
            for key, value in item.items():
                if isinstance(value, (list, dict)):
                    # Convert complex structures to string representation for now
                    flattened_item[key] = str(value)
                else:
                    flattened_item[key] = value
            chart_data.append(flattened_item)
    
    return pd.DataFrame(chart_data)

def generate_chart_format_with_mixtral(data, user_query):
    """Generate optimal chart format recommendation using Mixtral"""
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }
    
    # Prepare chart-friendly data for analysis
    chart_df = prepare_chart_data(data)
    chart_sample = chart_df.to_dict('records')[:3] if not chart_df.empty else []
    chart_columns = chart_df.columns.tolist() if not chart_df.empty else []
    
    prompt = f"""
    You are a data visualization expert. Based on the following data and user query, recommend the best chart type and configuration.
    
    User Query: "{user_query}"
    Chart Data Sample: {json.dumps(chart_sample, indent=2)}
    Available Chart Columns: {chart_columns}
    Chart DataFrame Shape: {chart_df.shape if not chart_df.empty else "Empty"}
    
    IMPORTANT: The data has been preprocessed for charting:
    - Nested structures like "top_categories" have been flattened
    - Each category-location combination is now a separate row
    - Numeric values like "sales" are now proper numbers for charting
    
    Analyze the data and provide recommendations in this exact JSON format:
    {{
        "chart_type": "bar|line|pie|scatter|area|heatmap",
        "x_axis": "field_name_for_x_axis",
        "y_axis": "field_name_for_y_axis", 
        "color_by": "field_name_for_grouping_or_null",
        "title": "Descriptive Chart Title",
        "reasoning": "Why this chart type is optimal for this data"
    }}
    
    For the common product categories by location data:
    - Use "category" for x-axis (shows different product categories)  
    - Use "sales" for y-axis (numeric sales values)
    - Use "location" for color grouping (different colors per location)
    - Bar chart is often best for categorical comparisons
    
    Consider:
    - Time series data → line/area charts
    - Categorical comparisons → bar charts  
    - Parts of whole → pie charts
    - Relationships → scatter plots
    - Geographic data → maps
    
    Return ONLY the JSON object, no other text.
    """
    
    try:
        response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers=headers,
            json={
                "model": "mistralai/mixtral-8x7b-instruct", 
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.2
            },
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content'].strip()
            
            # Extract JSON from response
            start = content.find('{')
            end = content.rfind('}') + 1
            
            if start != -1 and end > start:
                json_str = content[start:end]
                return json.loads(json_str)
        
        return None
    except Exception as e:
        st.error(f"Chart format generation error: {e}")
        return None

def call_aggregation_api(command, collection=None, limit=None):
    """Call the MongoDB aggregation API"""
    try:
        payload = {
            "command": command
        }
        
        # Add optional parameters only if provided
        if collection:
            payload["collection"] = collection
        if limit:
            payload["limit"] = limit
            
        response = requests.post(
            f"{API_BASE_URL}/aggregate/execute",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=120
        )
        
        if response.status_code == 200:
            api_response = response.json()
            
            # Debug only if no results
            if not api_response.get('success') or not api_response.get('results'):
                with st.expander("🔍 Debug Info (Empty Results)"):
                    st.write(f"**API URL:** `{API_BASE_URL}/aggregate/execute`")
                    st.write(f"**Payload:** `{payload}`")
                    st.write(f"**Response Status:** `{response.status_code}`")
                    st.write(f"**Success:** `{api_response.get('success', False)}`")
                    st.write(f"**Error:** `{api_response.get('error', 'No error message')}`")
                    if api_response.get('generated_pipeline'):
                        st.write(f"**Pipeline:** `{api_response.get('generated_pipeline')}`")
            
            return api_response
        else:
            st.error(f"API Error: {response.status_code} - {response.text}")
            return None
            
    except requests.exceptions.ConnectionError:
        st.error("❌ Cannot connect to API server. Make sure the API is running on http://localhost:5002")
        return None
    except Exception as e:
        st.error(f"Request error: {e}")
        return None

def process_nested_data(data):
    """Process nested data structures for better display in DataFrame"""
    if not data:
        return pd.DataFrame()
    
    processed_data = []
    
    for item in data:
        processed_item = {}
        
        for key, value in item.items():
            if isinstance(value, list) and len(value) > 0:
                # Handle arrays of objects (like top_categories)
                if isinstance(value[0], dict):
                    # Convert array of objects to a readable string
                    formatted_items = []
                    for i, obj in enumerate(value[:5]):  # Show top 5 items
                        if 'category' in obj and 'sales' in obj:
                            formatted_items.append(f"{i+1}. {obj['category']}: {obj['sales']:,.0f}")
                        elif 'category' in obj:
                            formatted_items.append(f"{i+1}. {obj['category']}")
                        else:
                            # Generic object display
                            obj_str = ", ".join([f"{k}: {v}" for k, v in obj.items()])
                            formatted_items.append(f"{i+1}. {obj_str}")
                    
                    processed_item[key] = "\n".join(formatted_items)
                    if len(value) > 5:
                        processed_item[key] += f"\n... and {len(value) - 5} more"
                else:
                    # Handle arrays of primitives
                    if len(value) <= 10:
                        processed_item[key] = ", ".join(map(str, value))
                    else:
                        processed_item[key] = ", ".join(map(str, value[:10])) + f"... and {len(value) - 10} more"
            elif isinstance(value, dict):
                # Handle nested objects
                obj_str = ", ".join([f"{k}: {v}" for k, v in value.items()])
                processed_item[key] = obj_str
            else:
                # Handle primitive values
                processed_item[key] = value
        
        processed_data.append(processed_item)
    
    return pd.DataFrame(processed_data)

def create_chart(data, chart_config):
    """Create chart based on Mixtral's recommendation"""
    if not data or not chart_config:
        return None
    
    # Use chart-friendly data preparation
    df = prepare_chart_data(data)
    
    if df.empty:
        st.warning("No data available for charting")
        return None
        
    chart_type = chart_config.get('chart_type', 'bar')
    x_col = chart_config.get('x_axis')
    y_col = chart_config.get('y_axis') 
    color_col = chart_config.get('color_by')
    title = chart_config.get('title', 'Data Visualization')
    
    # Validate columns exist in dataframe
    available_cols = df.columns.tolist()
    
    if x_col not in available_cols:
        x_col = available_cols[0] if available_cols else None
        
    if y_col not in available_cols:
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        y_col = numeric_cols[0] if numeric_cols else available_cols[1] if len(available_cols) > 1 else None
    
    # Handle color_col validation
    if color_col and (color_col == 'null' or color_col not in available_cols):
        color_col = None
    
    try:
        if chart_type == 'bar':
            fig = px.bar(df, x=x_col, y=y_col, color=color_col, title=title)
        elif chart_type == 'line':
            fig = px.line(df, x=x_col, y=y_col, color=color_col, title=title)
        elif chart_type == 'pie':
            fig = px.pie(df, names=x_col, values=y_col, title=title)
        elif chart_type == 'scatter':  
            fig = px.scatter(df, x=x_col, y=y_col, color=color_col, title=title)
        elif chart_type == 'area':
            fig = px.area(df, x=x_col, y=y_col, color=color_col, title=title)
        else:
            # Default to bar chart
            fig = px.bar(df, x=x_col, y=y_col, color=color_col, title=title)
            
        fig.update_layout(
            height=500,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='#fafafa'
        )
        return fig
        
    except Exception as e:
        st.error(f"Chart creation error: {e}")
        st.error(f"Available columns: {available_cols}")
        st.error(f"Trying to use - X: {x_col}, Y: {y_col}, Color: {color_col}")
        
        # Show debug info
        with st.expander("🔍 Debug: Chart Data Structure"):
            st.write("DataFrame shape:", df.shape)
            st.write("DataFrame columns:", df.columns.tolist())
            st.write("DataFrame dtypes:", df.dtypes.tolist())
            st.dataframe(df.head())
        
        return None

# ========== SIDEBAR ==========
st.sidebar.markdown("## ➕ Mulai Analisis Baru")
search_query = st.sidebar.text_input("🔍 Cari analisis...", placeholder="Ketik query pencarian...")

st.sidebar.markdown("### 📁 Template Queries")
st.sidebar.markdown("*Klik untuk menggunakan template:*")

if st.sidebar.button("📊 Penjualan per Lokasi 2025", use_container_width=True):
    st.session_state.query = "tampilkan penjualan per lokasi tahun 2025"
    st.rerun()
    
if st.sidebar.button("📈 Penjualan per Bulan 2025", use_container_width=True):
    st.session_state.query = "show sales by month for all months in 2025"
    st.rerun()
    
if st.sidebar.button("🏪 Performa Toko Juni", use_container_width=True):
    st.session_state.query = "tampilkan penjualan per lokasi bulan juni"
    st.rerun()

if st.sidebar.button("🛍️ Produk Terlaris per Bulan", use_container_width=True):
    st.session_state.query = "show top selling products by month"
    st.rerun()

if st.sidebar.button("💳 Analisis Metode Pembayaran", use_container_width=True):
    st.session_state.query = "analyze payment methods performance"
    st.rerun()

if st.sidebar.button("📊 Lokasi vs Bulan", use_container_width=True):
    st.session_state.query = "show sales by location grouped by month"
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("### ⚙️ System Status")

try:
    api_health = requests.get(f"{API_BASE_URL}/health", timeout=5)
    if api_health.status_code == 200:
        health_data = api_health.json()
        api_status = "🟢 Connected"
        mongodb_status = "🟢 Active" if health_data.get('services', {}).get('mongodb') else "🔴 Inactive"
    else:
        api_status = "🔴 Disconnected"
        mongodb_status = "❓ Unknown"
except:
    api_status = "🔴 Disconnected"
    mongodb_status = "❓ Unknown"

st.sidebar.markdown(f"""
<div class="info-box" style="font-size: 14px;">
    <div><strong>🌐 API Server:</strong> {api_status}</div>
    <div style="margin-top: 8px;"><strong>📊 MongoDB:</strong> {mongodb_status}</div>
    <div style="margin-top: 8px;"><strong>🔗 Endpoint:</strong> <code>:5002</code></div>
</div>
""", unsafe_allow_html=True)

# ========== MAIN AREA ==========
st.title("📊 Dashboard Analisis Penjualan dengan AI")
st.markdown("*Powered by MongoDB Aggregation + Claude + Mixtral*")

# Initialize session state
if 'query' not in st.session_state:
    st.session_state.query = ""
if 'api_response' not in st.session_state:
    st.session_state.api_response = None

# Query input
with st.container():
    col1, col2 = st.columns([4, 1])
    
    with col1:
        user_query = st.text_input(
            "💬 Tanyakan sesuatu tentang data Anda...", 
            value=st.session_state.query,
            placeholder="Contoh: Tampilkan penjualan per lokasi dan per bulan tahun 2025",
            key="main_query"
        )
    
    with col2:
        if st.button("📤 Analisis Data", type="primary", use_container_width=True):
            if user_query:
                with st.spinner("🔄 Memproses query..."):
                    st.session_state.api_response = call_aggregation_api(user_query)
                    st.session_state.query = user_query

# Display results if available
if st.session_state.api_response and st.session_state.api_response.get('success'):
    response = st.session_state.api_response
    
    # ========== METRICS ROW ==========
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="info-box">
            <div style="text-align: center;">
                <div style="font-size: 2.5rem; font-weight: bold; margin-bottom: 10px;">📊</div>
                <div style="font-size: 2rem; font-weight: bold; color: #00d4ff;">{response.get('total_results', 0)}</div>
                <div style="font-size: 1.1rem; color: #e0e6ed;">Total Results</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="info-box">
            <div style="text-align: center;">
                <div style="font-size: 2.5rem; font-weight: bold; margin-bottom: 10px;">⏱️</div>
                <div style="font-size: 2rem; font-weight: bold; color: #4ecdc4;">{response.get('execution_time', 0):.2f}s</div>
                <div style="font-size: 1.1rem; color: #e0e6ed;">Execution Time</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="info-box">
            <div style="text-align: center;">
                <div style="font-size: 2.5rem; font-weight: bold; margin-bottom: 10px;">💾</div>
                <div style="font-size: 2rem; font-weight: bold; color: #ff6b6b;">{response.get('documents_in_collection', 0):,}</div>
                <div style="font-size: 1.1rem; color: #e0e6ed;">Documents</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="info-box">
            <div style="text-align: center;">
                <div style="font-size: 2.5rem; font-weight: bold; margin-bottom: 10px;">🗃️</div>
                <div style="font-size: 1.5rem; font-weight: bold; color: #ffd93d;">{response.get('collection_used', 'N/A')}</div>
                <div style="font-size: 1.1rem; color: #e0e6ed;">Collection</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # ========== CHART AREA ==========
    results = response.get('results', [])
    
    if results:
        st.markdown("### 📈 Visualisasi Data")
        
        with st.spinner("🎨 Generating optimal chart..."):
            chart_config = generate_chart_format_with_mixtral(results, user_query)
        
        if chart_config:
            # Show chart configuration
            with st.expander("🎯 Chart Configuration (by Mixtral)"):
                col1, col2 = st.columns(2)
                with col1:
                    st.json({
                        "Chart Type": chart_config.get('chart_type'),
                        "X-Axis": chart_config.get('x_axis'),
                        "Y-Axis": chart_config.get('y_axis'),
                        "Color By": chart_config.get('color_by')
                    })
                with col2:
                    st.markdown(f"**Reasoning:** {chart_config.get('reasoning', 'N/A')}")
            
            # Create and display chart
            fig = create_chart(results, chart_config)
            if fig:
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Could not create chart with recommended format")
        
        # Raw data table
        with st.expander("📋 Raw Data"):
            # Check if data contains nested objects
            has_nested = any(
                isinstance(item.get(key), (list, dict)) 
                for item in results[:1] 
                for key in item.keys()
            ) if results else False
            
            if has_nested:
                st.info("🔧 Processing nested data for better display...")
                df = process_nested_data(results)
                st.dataframe(df, use_container_width=True)
                
                # Also show raw JSON for debugging
                with st.expander("🔍 Raw JSON (for debugging)"):
                    st.json(results[:3])  # Show first 3 items
            else:
                df = pd.DataFrame(results)
                st.dataframe(df, use_container_width=True)
    
    # ========== ANALYTICS DESCRIPTION ==========
    if response.get('description'):
        st.markdown("### 🧠 AI Analytics Insights")
        
        # Query and translation info
        st.markdown(f"""
        <div class="analytics-box">
            <div style="margin-bottom: 15px;">
                <strong style="color: #4ecdc4; font-size: 1.1rem;">📝 Original Query:</strong><br>
                <em style="color: #fafafa; font-size: 1.05rem;">{response.get('original_command', 'N/A')}</em>
            </div>
            <div style="margin-bottom: 20px;">
                <strong style="color: #00d4ff; font-size: 1.1rem;">🌍 Translation:</strong><br>
                <em style="color: #fafafa; font-size: 1.05rem;">{response.get('translated_command', 'N/A')}</em>
            </div>
            <div>
                <strong style="color: #ff6b6b; font-size: 1.2rem;">🧠 AI Analysis:</strong><br>
                <div style="margin-top: 10px; line-height: 1.8; font-size: 1.1rem;">
                    {response.get('description', 'No analysis available')}
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # ========== TECHNICAL DETAILS ==========
    with st.expander("🔧 Technical Details"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Generated Pipeline:**")
            st.json(response.get('generated_pipeline', {}))
        
        with col2:
            st.markdown("**Execution Details:**")
            st.write(f"- Execution time: {response.get('execution_time', 0):.3f} seconds")
            st.write(f"- Total results: {response.get('total_results', 0)} records")
            st.write(f"- Collection: {response.get('collection_used', 'N/A')}")
            st.write(f"- Documents in collection: {response.get('documents_in_collection', 0)}")

elif st.session_state.api_response and not st.session_state.api_response.get('success'):
    st.error(f"❌ Error: {st.session_state.api_response.get('error', 'Unknown error')}")

# ========== SAMPLE QUERIES ==========
if not st.session_state.api_response:
    st.markdown("### 💡 Contoh Query yang Dapat Anda Gunakan")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="info-box">
            <h4 style="color: #00d4ff; margin-bottom: 15px;">📊 Analisis Penjualan</h4>
            <ul style="line-height: 2; font-size: 15px;">
                <li>tampilkan penjualan per lokasi tahun 2025</li>
                <li>sales by location for June</li>
                <li>monthly revenue trend 2025</li>
                <li>penjualan harian bulan ini</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="info-box">
            <h4 style="color: #4ecdc4; margin-bottom: 15px;">🏆 Analisis Performa</h4>
            <ul style="line-height: 2; font-size: 15px;">
                <li>top performing locations</li>
                <li>penjualan tertinggi per bulan</li>
                <li>compare locations 2024 vs 2025</li>
                <li>produk dengan penjualan terbaik</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="info-box">
            <h4 style="color: #ff6b6b; margin-bottom: 15px;">🧠 Business Insights</h4>
            <ul style="line-height: 2; font-size: 15px;">
                <li>payment method analysis</li>
                <li>seasonal sales patterns</li>
                <li>product category performance</li>
                <li>customer behavior analysis</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

