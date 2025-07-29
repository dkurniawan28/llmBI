#!/usr/bin/env python3

import requests
import json
import time

class APITester:
    def __init__(self, base_url="http://localhost:5000"):
        self.base_url = base_url
    
    def test_health(self):
        """Test health endpoint"""
        print("🏥 Testing health endpoint...")
        try:
            response = requests.get(f"{self.base_url}/health")
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Health check passed: {data}")
                return True
            else:
                print(f"❌ Health check failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Health check error: {e}")
            return False
    
    def test_aggregate_endpoint(self, command, collection="transaction_sale", limit=5):
        """Test aggregate execution endpoint"""
        print(f"\n🤖 Testing aggregate with command: '{command}'")
        
        payload = {
            "command": command,
            "collection": collection,
            "limit": limit
        }
        
        try:
            start_time = time.time()
            response = requests.post(
                f"{self.base_url}/aggregate/execute",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=120
            )
            execution_time = time.time() - start_time
            
            print(f"⏱️  Request completed in {execution_time:.2f}s")
            print(f"📊 Response status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print("✅ Success!")
                print(f"   Original: {data.get('original_command')}")
                print(f"   Translated: {data.get('translated_command')}")
                print(f"   Pipeline: {json.dumps(data.get('generated_pipeline'), indent=2)}")
                print(f"   Results: {len(data.get('results', []))} records")
                print(f"   Sample result: {data.get('results', [{}])[0] if data.get('results') else 'No results'}")
                return True
            else:
                error_data = response.json() if response.headers.get('content-type') == 'application/json' else response.text
                print(f"❌ Failed: {error_data}")
                return False
                
        except Exception as e:
            print(f"❌ Request error: {e}")
            return False
    
    def test_predefined_pipelines(self):
        """Test predefined pipelines endpoint"""
        print("\n📋 Testing predefined pipelines...")
        
        try:
            # List available pipelines
            response = requests.get(f"{self.base_url}/aggregate/pipelines")
            if response.status_code == 200:
                data = response.json()
                pipelines = data.get('available_pipelines', [])
                print(f"✅ Available pipelines: {pipelines}")
                
                # Test first pipeline
                if pipelines:
                    pipeline_name = pipelines[0]
                    print(f"\n🔧 Testing pipeline: {pipeline_name}")
                    
                    response = requests.post(f"{self.base_url}/aggregate/pipelines/{pipeline_name}")
                    if response.status_code == 200:
                        result = response.json()
                        print(f"✅ Pipeline executed successfully!")
                        print(f"   Results: {len(result.get('results', []))} records")
                        return True
                    else:
                        print(f"❌ Pipeline execution failed: {response.status_code}")
                        return False
                        
            else:
                print(f"❌ Failed to get pipelines: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Predefined pipelines error: {e}")
            return False
    
    def run_all_tests(self):
        """Run all API tests"""
        print("🧪 Starting API Tests")
        print("=" * 60)
        
        tests = [
            ("Health Check", self.test_health),
            ("Indonesian Command", lambda: self.test_aggregate_endpoint("tampilkan penjualan per lokasi")),
            ("English Command", lambda: self.test_aggregate_endpoint("show sales by payment method")),
            ("Time-based Query", lambda: self.test_aggregate_endpoint("penjualan harian minggu ini")),
            ("Product Analysis", lambda: self.test_aggregate_endpoint("produk terlaris bulan ini")),
            ("Predefined Pipelines", self.test_predefined_pipelines)
        ]
        
        results = []
        for test_name, test_func in tests:
            print(f"\n{'='*20} {test_name} {'='*20}")
            try:
                success = test_func()
                results.append((test_name, success))
            except Exception as e:
                print(f"❌ Test '{test_name}' crashed: {e}")
                results.append((test_name, False))
        
        # Summary
        print(f"\n{'='*60}")
        print("📊 TEST SUMMARY")
        print(f"{'='*60}")
        
        passed = sum(1 for _, success in results if success)
        total = len(results)
        
        for test_name, success in results:
            status = "✅ PASSED" if success else "❌ FAILED"
            print(f"{test_name:.<30} {status}")
        
        print(f"\nOverall: {passed}/{total} tests passed")
        
        if passed == total:
            print("🎉 All tests passed!")
        else:
            print("⚠️  Some tests failed. Check the logs above.")
        
        return passed == total

def main():
    print("🚀 API Test Suite for Transaction Sale Aggregate API")
    print("📝 Make sure the API server is running on localhost:5000")
    print("🔑 Ensure OPENROUTER_API_KEY environment variable is set")
    
    # Wait for user confirmation
    input("\nPress Enter to start testing...")
    
    tester = APITester()
    success = tester.run_all_tests()
    
    return 0 if success else 1

if __name__ == "__main__":
    exit(main())