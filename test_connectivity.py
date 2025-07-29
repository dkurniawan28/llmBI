#!/usr/bin/env python3

import requests
import time

def test_connectivity():
    print("🔗 Testing service connectivity...")
    
    # Test API server
    try:
        api_response = requests.get("http://localhost:5002/health", timeout=5)
        if api_response.status_code == 200:
            print("✅ API Server: http://localhost:5002 - WORKING")
        else:
            print(f"❌ API Server: http://localhost:5002 - Status {api_response.status_code}")
    except Exception as e:
        print(f"❌ API Server: http://localhost:5002 - ERROR: {e}")
    
    # Test Streamlit
    try:
        streamlit_response = requests.get("http://localhost:8501", timeout=5)
        if streamlit_response.status_code == 200:
            print("✅ Streamlit: http://localhost:8501 - WORKING")
        else:
            print(f"❌ Streamlit: http://localhost:8501 - Status {streamlit_response.status_code}")
    except Exception as e:
        print(f"❌ Streamlit: http://localhost:8501 - ERROR: {e}")
    
    # Test API call with the specific query
    try:
        test_payload = {"command": "berikan data 10 product terbanyak dari 10 lokasi dengan penjualan terbesar"}
        api_test = requests.post(
            "http://localhost:5002/aggregate/execute",
            json=test_payload,
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        if api_test.status_code == 200:
            data = api_test.json()
            if data.get('success') and data.get('results'):
                print(f"✅ API Query Test: SUCCESS - {len(data.get('results', []))} results")
            else:
                print(f"❌ API Query Test: FAILED - {data.get('error', 'Unknown error')}")
        else:
            print(f"❌ API Query Test: HTTP {api_test.status_code}")
            
    except Exception as e:
        print(f"❌ API Query Test: ERROR: {e}")

if __name__ == "__main__":
    test_connectivity()
    print("\n💡 If both services are working, try:")
    print("   1. Clear browser cache (Ctrl+Shift+R / Cmd+Shift+R)")
    print("   2. Open http://localhost:8501 in private/incognito mode")
    print("   3. Check browser console for JavaScript errors")
    print("   4. Try the query: 'berikan data 10 product terbanyak dari 10 lokasi dengan penjualan terbesar'")