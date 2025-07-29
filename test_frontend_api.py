#!/usr/bin/env python3

import requests
import json

# Test the API call that Streamlit makes
def test_api_call():
    API_BASE_URL = "http://localhost:5002"
    
    payload = {
        "command": "tampilkan top 10 kategori produk per lokasi untuk bulan juni"
    }
    
    try:
        print("🧪 Testing API call...")
        response = requests.post(
            f"{API_BASE_URL}/aggregate/execute",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=120
        )
        
        print(f"📊 Response Status: {response.status_code}")
        
        if response.status_code == 200:
            api_response = response.json()
            print(f"✅ Success: {api_response.get('success')}")
            print(f"📝 Results count: {len(api_response.get('results', []))}")
            print(f"🏪 Collection used: {api_response.get('collection_used')}")
            print(f"⏱️ Execution time: {api_response.get('execution_time'):.2f}s")
            
            results = api_response.get('results', [])
            if results:
                print(f"🔍 First result structure: {list(results[0].keys())}")
                
                # Test prepare_chart_data logic
                chart_data = []
                for item in results[:3]:  # Test with first 3 items
                    if 'location' in item and 'top_categories' in item:
                        location = item['location']
                        categories = item.get('top_categories', [])
                        
                        for category_info in categories[:5]:  # Top 5 for test
                            if isinstance(category_info, dict) and 'category' in category_info and 'sales' in category_info:
                                chart_data.append({
                                    'location': location,
                                    'category': category_info['category'],
                                    'sales': float(category_info['sales']) if category_info['sales'] else 0
                                })
                
                print(f"📈 Chart data rows generated: {len(chart_data)}")
                if chart_data:
                    print(f"🎯 Sample chart data: {chart_data[0]}")
                    print(f"💰 Sales data type: {type(chart_data[0]['sales'])}")
                
                return True
            else:
                print("❌ No results returned")
                return False
        else:
            print(f"❌ API Error: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"💥 Error: {e}")
        return False

if __name__ == "__main__":
    success = test_api_call()
    print(f"\n🎯 Test Result: {'PASSED' if success else 'FAILED'}")