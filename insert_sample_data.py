#!/usr/bin/env python3

from mongodb_connection import MongoDBSSHConnection
import json
from datetime import datetime, timedelta
import random

def generate_manual_sample_data(num_records=10):
    """Generate sample data without AI"""
    
    locations = ["Jakarta Pusat", "Bandung Kopo", "Surabaya Timur", "Yogyakarta", "Medan Plaza"]
    products = [
        {"category": "Tea Series", "name": "Es Teh Manis", "price": 12000},
        {"category": "Coffee Series", "name": "Kopi Susu", "price": 18000},
        {"category": "Food", "name": "Roti Bakar", "price": 15000},
        {"category": "Dessert", "name": "Es Krim Vanilla", "price": 20000},
        {"category": "Tea Series", "name": "Teh Tarik", "price": 16000}
    ]
    payment_methods = ["Cash", "QRIS", "Debit Card"]
    
    sample_data = []
    
    for i in range(num_records):
        # Random date in June 2024
        base_date = datetime(2024, 6, 1)
        random_days = random.randint(0, 29)
        sale_date = base_date + timedelta(days=random_days)
        
        product = random.choice(products)
        location = random.choice(locations)
        qty = random.randint(1, 3)
        gross_sales = product["price"] * qty
        total = gross_sales + random.randint(0, 2000)  # Add small variation
        
        record = {
            "Location Name": location,
            "Receipt No": f"R{1000 + i}",
            "Sales no": f"S{2000 + i}",
            "Sales Date": sale_date.strftime("%d/%m/%Y"),
            "Sales Time": f"{random.randint(8, 21):02d}:{random.randint(0, 59):02d}:00",
            "Customer Name": f"Customer {i+1}" if random.random() > 0.3 else None,
            "Customer Phone No": f"08{random.randint(1000000000, 9999999999)}" if random.random() > 0.5 else None,
            "No. of Guest": random.randint(1, 4),
            "Order Type": random.choice(["Dine-in", "Take Away"]),
            "Product Category Name": product["category"],
            "Product Name": product["name"],
            "Product qty": qty,
            "Modifiers": "Normal Sugar" if random.random() > 0.5 else "Less Sugar",
            "Cancelled Quantity": 0,
            "Cancel reasons": 0,
            "Cancelled By": 0,
            "Price": product["price"],
            "Add On Price": 0,
            "Gross Sales": str(gross_sales),
            "Discount": "0",
            "Surcharge": "0",
            "Net Sales": str(gross_sales),
            "Service Charge": 0,
            "Service Charge Tax": 0,
            "Product Tax": str(int(gross_sales * 0.1)),
            "Total Tax": str(int(total * 0.1)),
            "Tax Name": "PB1",
            "Additional Charge Fee": 0,
            "Delivery Method": 0,
            "Delivery Fee": 0,
            "Rounding": 0,
            "Total": str(total),
            "Void Total": None,
            "Promo Name": None,
            "Promo Subsidized": 0,
            "Processing Fee": 0,
            "Net Received": str(total),
            "Payment Method": random.choice(payment_methods),
            "Payment Note": 0,
            "Adjustment Note": 0,
            "Device Name": "POS Terminal 1",
            "Preparation Time": random.randint(300, 900),
            "Serving Time": random.randint(60, 300),
            "Cashier Name": f"Kasir {random.randint(1, 5)}",
            "Waiter Name": random.randint(1, 10),
            "Status": "Paid",
            "Void Date": None,
            "Void at": None,
            "Void by": None,
            "Void notes": None
        }
        
        sample_data.append(record)
    
    return sample_data

def main():
    print("📝 Inserting Sample Data to MongoDB")
    print("=" * 40)
    
    try:
        # Connect to MongoDB
        mongo_conn = MongoDBSSHConnection()
        client = mongo_conn.connect()
        
        if not client:
            print("❌ Failed to connect to MongoDB")
            return
        
        print("✅ Connected to MongoDB")
        
        # Check existing data
        db = mongo_conn.get_database()
        collection = db['transaction_sale']
        existing_count = collection.count_documents({})
        
        print(f"📊 Existing records: {existing_count}")
        
        # Generate sample data
        print("🏭 Generating sample data...")
        sample_data = generate_manual_sample_data(20)
        
        # Insert data
        print(f"💾 Inserting {len(sample_data)} records...")
        collection.insert_many(sample_data)
        
        # Verify insertion
        new_count = collection.count_documents({})
        print(f"✅ Inserted successfully! Total records: {new_count}")
        
        # Show sample aggregation
        print("\n📈 Sample aggregation - Sales by Location:")
        pipeline = [
            {"$group": {"_id": "$Location Name", "total_sales": {"$sum": {"$toDouble": "$Total"}}, "count": {"$sum": 1}}},
            {"$sort": {"total_sales": -1}}
        ]
        
        results = list(collection.aggregate(pipeline))
        for result in results:
            print(f"  {result['_id']}: Rp {result['total_sales']:,.0f} ({result['count']} transactions)")
        
        mongo_conn.disconnect()
        print("\n🎉 Sample data insertion complete!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()