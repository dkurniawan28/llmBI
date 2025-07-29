#!/usr/bin/env python3

import subprocess
import time
import requests
import signal
import sys
import os

def start_api_server():
    """Start the API server"""
    print("🚀 Starting API Server...")
    try:
        api_process = subprocess.Popen([
            sys.executable, "api_server.py"
        ], cwd="/Users/dedykurniawan/Documents/BI & Analitics")
        
        # Wait a moment for startup
        time.sleep(3)
        
        # Test API health
        try:
            response = requests.get("http://localhost:5001/health", timeout=5)
            if response.status_code == 200:
                print("✅ API Server running on http://localhost:5001")
                return api_process
            else:
                print(f"❌ API Server health check failed: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"❌ API Server not responding: {e}")
            return None
            
    except Exception as e:
        print(f"❌ Failed to start API server: {e}")
        return None

def start_streamlit():
    """Start Streamlit dashboard"""
    print("📊 Starting Streamlit Dashboard...")
    try:
        streamlit_process = subprocess.Popen([
            sys.executable, "-m", "streamlit", "run", 
            "FE/streamlit_fe.py",
            "--server.port", "8503",
            "--server.address", "localhost", 
            "--server.headless", "true",
            "--browser.gatherUsageStats", "false"
        ], cwd="/Users/dedykurniawan/Documents/BI & Analitics")
        
        # Wait for startup
        time.sleep(5)
        
        # Test Streamlit health
        try:
            response = requests.get("http://localhost:8503", timeout=10)
            if response.status_code == 200:
                print("✅ Streamlit Dashboard running on http://localhost:8503")
                return streamlit_process
            else:
                print(f"⚠️  Streamlit started but returned: {response.status_code}")
                return streamlit_process
        except requests.exceptions.RequestException as e:
            print(f"⚠️  Streamlit started but not responding yet: {e}")
            print("   Dashboard may still be loading...")
            return streamlit_process
            
    except Exception as e:
        print(f"❌ Failed to start Streamlit: {e}")  
        return None

def monitor_services(api_process, streamlit_process):
    """Monitor both services"""
    print("\n" + "="*60)
    print("🖥️  SERVICES MONITORING")
    print("="*60)
    print("📡 API Server: http://localhost:5001")
    print("📊 Dashboard: http://localhost:8503")
    print("📚 API Docs: http://localhost:5001/docs")
    print("\nPress Ctrl+C to stop all services")
    print("="*60)
    
    try:
        while True:
            time.sleep(10)
            
            # Check API health
            try:
                api_response = requests.get("http://localhost:5001/health", timeout=3)
                api_status = "🟢 UP" if api_response.status_code == 200 else "🔴 DOWN"
            except:
                api_status = "🔴 DOWN"
            
            # Check Streamlit health  
            try:
                st_response = requests.get("http://localhost:8503", timeout=3)
                st_status = "🟢 UP" if st_response.status_code == 200 else "🟡 LOADING"
            except:
                st_status = "🔴 DOWN"
            
            print(f"Status - API: {api_status} | Dashboard: {st_status}")
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping services...")
        
        if api_process:
            api_process.terminate()
            print("✅ API Server stopped")
            
        if streamlit_process:
            streamlit_process.terminate()
            print("✅ Streamlit Dashboard stopped")
            
        print("👋 All services stopped")

def main():
    print("🏃‍♂️ Starting Analytics Platform...")
    
    # Start API server
    api_process = start_api_server()
    if not api_process:
        print("❌ Cannot start API server. Exiting.")
        return
    
    # Start Streamlit
    streamlit_process = start_streamlit()
    if not streamlit_process:
        print("❌ Cannot start Streamlit. Stopping API server.")
        api_process.terminate()
        return
    
    # Monitor both services
    monitor_services(api_process, streamlit_process)

if __name__ == "__main__":
    main()