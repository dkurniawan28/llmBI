#!/bin/bash

echo "🚀 Starting Streamlit Frontend"
echo "==============================="

# Check if virtual environment exists
cd "/Users/dedykurniawan/Documents/BI & Analitics"

if [ ! -d "bi" ]; then
    echo "❌ Virtual environment 'bi' not found!"
    echo "Please create it first with: python -m venv bi"
    exit 1
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source bi/bin/activate

# Install/update frontend dependencies
echo "📦 Installing Streamlit dependencies..."
pip install -r FE/requirements.txt

# Check API status
echo "🔍 Checking API status..."
curl -s http://localhost:5001/health > /dev/null
if [ $? -eq 0 ]; then
    echo "✅ API server is running on localhost:5001"
else
    echo "⚠️  API server not detected on localhost:5001"
    echo "   Make sure to start the API server first with: python api_server.py"
fi

# Start Streamlit
echo "🌐 Starting Streamlit dashboard..."
echo "📱 Dashboard will be available at: http://localhost:8501"
echo "🔗 API endpoint: http://localhost:5001"
echo ""
echo "Press Ctrl+C to stop the dashboard"
echo ""

streamlit run FE/streamlit_fe.py --server.port 8501 --server.headless true --browser.serverAddress localhost --browser.gatherUsageStats false