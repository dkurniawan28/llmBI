#!/usr/bin/env python3

import requests
import json
import pandas as pd

# Test the exact same API call that Streamlit makes
def test_streamlit_api():
    API_BASE_URL = "http://localhost:5002"
    
    command = "berikan data 10 product terbanyak dari 10 lokasi dengan penjualan terbesar"
    
    # This is exactly what Streamlit does
    payload = {
        "command": command
    }
    
    try:
        print("🧪 Testing Streamlit API call...")
        print(f"📝 Command: {command}")
        print(f"🌐 URL: {API_BASE_URL}/aggregate/execute")
        print(f"📦 Payload: {payload}")
        
        response = requests.post(
            f"{API_BASE_URL}/aggregate/execute",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=120
        )
        
        print(f"\n📊 Response Status: {response.status_code}")
        
        if response.status_code == 200:
            api_response = response.json()
            print(f"✅ Success: {api_response.get('success')}")
            
            results = api_response.get('results', [])
            print(f"📈 Results count: {len(results)}")
            
            if results:
                print(f"🔍 First result keys: {list(results[0].keys())}")
                
                # Test the chart data preparation logic from Streamlit
                print("\n📊 Testing chart data preparation...")
                chart_data = []
                
                for item in results:
                    if 'location' in item and 'top_products' in item:
                        location = item['location']
                        location_total = float(item.get('location_total', 0))
                        products = item.get('top_products', [])
                        
                        print(f"🏪 Processing location: {location} (Total: {location_total:,.0f})")
                        print(f"🛍️ Products found: {len(products)}")
                        
                        for product_info in products[:3]:  # Test with first 3
                            if isinstance(product_info, dict):
                                chart_row = {
                                    'location': location,
                                    'location_total': location_total,
                                    'product_name': product_info.get('product_name', 'Unknown'),
                                    'product_category': product_info.get('product_category', 'Unknown'),
                                    'revenue': float(product_info.get('revenue', 0)),
                                    'quantity': int(product_info.get('quantity', 0))
                                }
                                chart_data.append(chart_row)
                                print(f"  📦 {chart_row['product_name']}: {chart_row['revenue']:,.0f}")
                
                print(f"\n📈 Chart data rows created: {len(chart_data)}")
                
                if chart_data:
                    df = pd.DataFrame(chart_data)
                    print(f"📊 DataFrame shape: {df.shape}")
                    print(f"📋 DataFrame columns: {df.columns.tolist()}")
                    print("\n🎯 Sample chart data:")
                    print(df.head(3).to_string(index=False))
                    
                    # Check if the data has proper types
                    print(f"\n🔍 Data types:")
                    print(f"  Revenue type: {type(chart_data[0]['revenue'])}")
                    print(f"  Location total type: {type(chart_data[0]['location_total'])}")
                    
                    return True
                else:
                    print("❌ No chart data generated")
                    return False
            else:
                print("❌ No results in API response")
                # Debug the API response
                print(f"🔍 Full API response: {json.dumps(api_response, indent=2)[:500]}")
                return False
        else:
            print(f"❌ API Error: {response.status_code}")
            print(f"📝 Response text: {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"💥 Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_streamlit_api()
    print(f"\n🎯 Test Result: {'PASSED' if success else 'FAILED'}")