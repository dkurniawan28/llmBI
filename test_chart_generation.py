#!/usr/bin/env python3

import requests
import json
import pandas as pd
import os

# Test chart generation process
def test_chart_generation():
    API_BASE_URL = "http://localhost:5002"
    OPENROUTER_API_KEY = "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"
    
    # Step 1: Get the actual data from API
    print("🔍 Step 1: Getting data from API...")
    payload = {"command": "berikan data 10 product terbanyak dari 10 lokasi dengan penjualan terbesar"}
    
    response = requests.post(
        f"{API_BASE_URL}/aggregate/execute",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=120
    )
    
    if response.status_code != 200:
        print(f"❌ API failed: {response.status_code}")
        return False
    
    api_data = response.json()
    results = api_data.get('results', [])
    
    if not results:
        print("❌ No results from API")
        return False
        
    print(f"✅ Got {len(results)} results")
    
    # Step 2: Prepare chart data (same as Streamlit)
    print("\n📊 Step 2: Preparing chart data...")
    chart_data = []
    
    for item in results:
        if 'location' in item and 'top_products' in item:
            location = item['location']
            location_total = float(item.get('location_total', 0))
            products = item.get('top_products', [])
            
            for product_info in products[:10]:
                if isinstance(product_info, dict):
                    chart_data.append({
                        'location': location,
                        'location_total': location_total,
                        'product_name': product_info.get('product_name', 'Unknown'),
                        'product_category': product_info.get('product_category', 'Unknown'),
                        'revenue': float(product_info.get('revenue', 0)),
                        'quantity': int(product_info.get('quantity', 0))
                    })
    
    print(f"✅ Created {len(chart_data)} chart rows")
    
    if not chart_data:
        print("❌ No chart data created")
        return False
    
    # Step 3: Test Mixtral chart recommendation
    print("\n🤖 Step 3: Testing Mixtral chart recommendation...")
    
    chart_df = pd.DataFrame(chart_data)
    chart_sample = chart_df.to_dict('records')[:3]
    chart_columns = chart_df.columns.tolist()
    
    mixtral_prompt = f"""
    You are a data visualization expert. Based on the following data and user query, recommend the best chart type and configuration.
    
    User Query: "berikan data 10 product terbanyak dari 10 lokasi dengan penjualan terbesar"
    Chart Data Sample: {json.dumps(chart_sample, indent=2)}
    Available Chart Columns: {chart_columns}
    Chart DataFrame Shape: {chart_df.shape}
    
    IMPORTANT: The data has been preprocessed for charting:
    - Nested structures like "top_products" have been flattened
    - Each product-location combination is now a separate row
    - Numeric values like "revenue" are now proper numbers for charting
    
    Analyze the data and provide recommendations in this exact JSON format:
    {{
        "chart_type": "bar|line|pie|scatter|area|heatmap",
        "x_axis": "field_name_for_x_axis",
        "y_axis": "field_name_for_y_axis", 
        "color_by": "field_name_for_grouping_or_null",
        "title": "Descriptive Chart Title",
        "reasoning": "Why this chart type is optimal for this data"
    }}
    
    For the common data patterns:
    - Product categories by location: Use "category" (x-axis), "sales" (y-axis), "location" (color)
    - Top products by location: Use "product_name" (x-axis), "revenue" (y-axis), "location" (color)  
    - Location performance: Use "location" (x-axis), "location_total" (y-axis)
    - Bar chart is often best for categorical comparisons
    
    Return ONLY the JSON object, no other text.
    """
    
    try:
        mixtral_response = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "mistralai/mixtral-8x7b-instruct", 
                "messages": [{"role": "user", "content": mixtral_prompt}],
                "temperature": 0.2
            },
            timeout=30
        )
        
        if mixtral_response.status_code == 200:
            result = mixtral_response.json()
            content = result['choices'][0]['message']['content'].strip()
            
            print(f"📝 Mixtral raw response: {content[:200]}...")
            
            # Extract JSON from response
            start = content.find('{')
            end = content.rfind('}') + 1
            
            if start != -1 and end > start:
                json_str = content[start:end]
                chart_config = json.loads(json_str)
                
                print("✅ Chart configuration generated:")
                print(json.dumps(chart_config, indent=2))
                
                # Step 4: Test chart creation logic
                print("\n📈 Step 4: Testing chart creation...")
                
                x_col = chart_config.get('x_axis')
                y_col = chart_config.get('y_axis')
                color_col = chart_config.get('color_by')
                
                print(f"📊 Chart config: X={x_col}, Y={y_col}, Color={color_col}")
                print(f"📋 Available columns: {chart_df.columns.tolist()}")
                
                # Validate columns
                if x_col in chart_df.columns and y_col in chart_df.columns:
                    print("✅ Chart columns are valid")
                    print(f"📊 X-axis data type: {chart_df[x_col].dtype}")
                    print(f"📊 Y-axis data type: {chart_df[y_col].dtype}")
                    
                    if color_col and color_col in chart_df.columns:
                        print(f"🎨 Color column valid: {chart_df[color_col].dtype}")
                    
                    print(f"🎯 Sample data for chart:")
                    print(chart_df[[x_col, y_col] + ([color_col] if color_col and color_col in chart_df.columns else [])].head(3))
                    
                    return True
                else:
                    print(f"❌ Invalid columns: X={x_col in chart_df.columns}, Y={y_col in chart_df.columns}")
                    return False
                    
            else:
                print(f"❌ Could not extract JSON from Mixtral response")
                print(f"Raw content: {content}")
                return False
        else:
            print(f"❌ Mixtral API failed: {mixtral_response.status_code}")
            print(f"Response: {mixtral_response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"❌ Chart generation error: {e}")
        return False

if __name__ == "__main__":
    success = test_chart_generation()
    print(f"\n🎯 Chart Generation Test: {'PASSED' if success else 'FAILED'}")