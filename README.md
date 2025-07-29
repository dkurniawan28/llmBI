# LLM-Powered MongoDB Business Intelligence System

A sophisticated business intelligence system that combines MongoDB aggregation pipelines with Large Language Models (Claude & Mixtral) for natural language query processing and intelligent data analysis.

## 🚀 Features

- **Natural Language Queries**: Ask questions in Indonesian or English
- **AI-Powered Aggregation**: Claude generates MongoDB pipelines from natural language
- **Smart Analytics**: Mixtral provides business insights and translations
- **Optimized Performance**: Pre-aggregated collections for 3.6M+ records
- **Real-time Visualization**: Streamlit frontend with dark theme and charts
- **Mixed Data Format Support**: Handles inconsistent date formats and number formats

## 🏗️ Architecture

```
User Query → Streamlit Frontend → Flask API → Claude (Pipeline Gen) → MongoDB → Mixtral (Analysis) → Results
```

## 📊 Data Collections

- **transaction_sales**: 3,643,374 raw transaction records
- **sales_by_location**: Aggregated sales by store location
- **sales_by_month**: Monthly sales trends
- **sales_by_location_month**: Location-month combinations
- **sales_by_product**: Product performance analytics
- **sales_by_payment_method**: Payment method analysis
- **sales_summary_nested**: Hierarchical sales summaries
- **product_performance_nested**: Multi-dimensional product analysis

## 🔧 Quick Start

1. **Start the API Server**:
   ```bash
   python3 api_server.py
   ```

2. **Start the Frontend**:
   ```bash
   cd FE && streamlit run streamlit_fe.py --server.port 8501
   ```

3. **Access the Application**:
   - Frontend: http://localhost:8501
   - API Docs: http://localhost:5002/docs

## 💡 Example Queries

- "tampilkan penjualan per lokasi dikelompokan per bulan"
- "show top 10 product categories by location for June"
- "monthly revenue trend for 2025"
- "payment method analysis"

## 🛠️ Technical Highlights

### Date Format Handling
- **Challenge**: Mixed date formats (string "DD/MM/YYYY" vs datetime objects)
- **Solution**: Intelligent date parsing pipeline with fallback mechanisms
- **Result**: Consistent month/year extraction across all collections

### Number Format Processing
- **Challenge**: Indonesian decimal format (comma as decimal separator)
- **Solution**: $replaceAll operations before $toDouble conversions
- **Result**: Accurate numeric calculations and aggregations

### Performance Optimization
- **Challenge**: 3.6M+ records causing slow queries
- **Solution**: Pre-aggregated collections with nested sub-documents
- **Result**: Sub-second response times for complex analytics

## 📈 Data Quality Fixes

✅ **Mixed Date Formats**: Standardized date parsing across all collections  
✅ **Decimal Format**: Proper handling of Indonesian number formatting  
✅ **Collection Routing**: Smart selection of optimal collections for queries  
✅ **Nested Data Display**: Enhanced frontend display of complex objects  
✅ **Chart Generation**: Fixed chart creation from nested data structures  

## 🔗 Repository

This system processes real tea shop transaction data with AI-powered natural language query capabilities, providing comprehensive business intelligence insights through an intuitive interface.

---

*Built with Flask, Streamlit, MongoDB, Claude AI, and Mixtral*
EOF < /dev/null