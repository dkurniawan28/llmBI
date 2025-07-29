#!/usr/bin/env python3

from mongodb_connection import MongoDBSSHConnection
from datetime import datetime
import random

def add_2025_sample_data():
    print("📝 Adding 2025 sample data...")
    
    mongo_conn = MongoDBSSHConnection()
    
    try:
        client = mongo_conn.connect()
        db = mongo_conn.get_database()
        collection = db['transaction_sale']
        
        locations = ["Jakarta Pusat", "Bandung Kopo", "Surabaya Timur", "Yogyakarta", "Medan Plaza"]
        products = [
            {"category": "Tea Series", "name": "Es Teh Manis", "price": 15000},
            {"category": "Coffee Series", "name": "Kopi Susu", "price": 22000},
            {"category": "Food", "name": "Roti Bakar", "price": 18000},
        ]
        payment_methods = ["Cash", "QRIS", "Debit Card"]
        
        sample_data_2025 = []
        
        # Generate data for different months in 2025
        months = [1, 2, 3, 4, 5]  # Jan to May 2025
        
        record_id = 3000  # Start from 3000 to avoid conflicts
        
        for month in months:
            for _ in range(8):  # 8 records per month
                # Random date in the month
                day = random.randint(1, 28)
                sale_date = f"{day:02d}/{month:02d}/2025"
                
                product = random.choice(products)
                location = random.choice(locations)
                qty = random.randint(1, 3)
                gross_sales = product["price"] * qty
                total = gross_sales + random.randint(0, 3000)
                
                record = {
                    "Location Name": location,
                    "Receipt No": f"R{record_id}",
                    "Sales no": f"S{record_id}",
                    "Sales Date": sale_date,
                    "Sales Time": f"{random.randint(8, 21):02d}:{random.randint(0, 59):02d}:00",
                    "Customer Name": f"Customer {record_id}" if random.random() > 0.3 else None,
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
                    "Void notes": None,
                    # Add month and year fields
                    "month": month,
                    "year": 2025
                }
                
                sample_data_2025.append(record)
                record_id += 1
        
        # Insert the data
        if sample_data_2025:
            result = collection.insert_many(sample_data_2025)
            print(f"✅ Inserted {len(result.inserted_ids)} records for 2025")
            
            # Verify insertion
            count_2025 = collection.count_documents({"year": 2025})
            total_count = collection.count_documents({})
            
            print(f"📊 Total documents now: {total_count}")
            print(f"📅 2025 documents: {count_2025}")
            
            # Show summary by month and location
            summary_pipeline = [
                {"$match": {"year": 2025}},
                {"$group": {
                    "_id": {"location": "$Location Name", "month": "$month"},
                    "count": {"$sum": 1},
                    "total_sales": {"$sum": {"$toDouble": "$Total"}}
                }},
                {"$sort": {"_id.location": 1, "_id.month": 1}}
            ]
            
            summary = list(collection.aggregate(summary_pipeline))
            print(f"\n📈 2025 Data Summary:")
            for item in summary:
                loc = item['_id']['location']
                month = item['_id']['month']
                count = item['count']
                sales = item['total_sales']
                print(f"   {loc} - Month {month}: {count} records, Rp {sales:,.0f}")
        
        mongo_conn.disconnect()
        print("\n🎉 2025 data added successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    add_2025_sample_data()