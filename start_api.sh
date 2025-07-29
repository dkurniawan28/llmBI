#!/bin/bash

echo "🚀 Starting Transaction Sale Aggregate API"
echo "=================================="

# Check if virtual environment exists
if [ ! -d "bi" ]; then
    echo "❌ Virtual environment 'bi' not found!"
    echo "Please create it first with: python -m venv bi"
    exit 1
fi

# Check if OPENROUTER_API_KEY is set
if [ -z "$OPENROUTER_API_KEY" ]; then
    echo "⚠️  OPENROUTER_API_KEY environment variable not set!"
    echo "Please set it with: export OPENROUTER_API_KEY='your_api_key_here'"
    echo "Continuing anyway..."
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source bi/bin/activate

# Install/update dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Start the API server
echo "🌐 Starting API server on http://localhost:5000"
echo "📚 Swagger documentation will be available at: http://localhost:5000/docs"
echo "🏥 Health check available at: http://localhost:5000/health"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

python api_server.py