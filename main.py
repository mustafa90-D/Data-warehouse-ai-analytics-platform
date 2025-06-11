#!/usr/bin/env python3
"""
Data Warehouse Project - Main Application
Runs the complete ETL pipeline and sets up the data warehouse
"""
import sys
import os
from datetime import datetime

# Add the project root to the path
sys.path.append(os.path.dirname(__file__))

from config.database import create_tables, get_database_session
from src.extractors.api_extractor import APIExtractor
from src.models.warehouse import DimUser, DimProduct, DimDate, FactSales

def setup_database():
    """Initialize database tables"""
    print("🗄️ Setting up database tables...")
    try:
        create_tables()
        print("✅ Database tables created successfully!")
        return True
    except Exception as e:
        print(f"❌ Error setting up database: {e}")
        return False

def load_sample_data():
    """Load sample data into the warehouse"""
    print("\n📥 Loading sample data...")
    
    try:
        # Extract data
        extractor = APIExtractor()
        data = extractor.extract_all_data()
        
        # Get database session
        session = get_database_session()
        
        # Clear existing data (to avoid duplicates)
        print("🗑️ Clearing existing data...")
        session.query(FactSales).delete()
        session.query(DimUser).delete()
        session.query(DimProduct).delete()
        session.query(DimDate).delete()
        session.commit()
        print("✅ Existing data cleared")
        
        # Load users
        print("👥 Loading users...")
        for user_data in data['users'][:5]:  # Load first 5 users
            user = DimUser(
                user_id=user_data['id'],
                name=user_data['name'],
                email=user_data['email'],
                city=user_data['address']['city'],
                phone=user_data['phone'],
                company=user_data['company']['name']
            )
            session.add(user)
        
        # Load products
        print("📦 Loading products...")
        for product_data in data['products']:
            product = DimProduct(
                product_id=product_data['id'],
                product_name=product_data['name'],
                category=product_data['category'],
                price=product_data['price']
            )
            session.add(product)
        
        # Load dates
        print("📅 Loading date dimension...")
        dates_to_add = [
            {"date_id": 20240101, "date": "2024-01-01", "year": 2024, "month": 1, "day": 1, "quarter": 1, "weekday": "Monday"},
            {"date_id": 20240102, "date": "2024-01-02", "year": 2024, "month": 1, "day": 2, "quarter": 1, "weekday": "Tuesday"},
            {"date_id": 20240103, "date": "2024-01-03", "year": 2024, "month": 1, "day": 3, "quarter": 1, "weekday": "Wednesday"},
            {"date_id": 20240601, "date": "2024-06-01", "year": 2024, "month": 6, "day": 1, "quarter": 2, "weekday": "Saturday"},
            {"date_id": 20241201, "date": "2024-12-01", "year": 2024, "month": 12, "day": 1, "quarter": 4, "weekday": "Sunday"},
        ]
        
        for date_data in dates_to_add:
            date_dim = DimDate(
                date_id=date_data['date_id'],
                date=datetime.strptime(date_data['date'], '%Y-%m-%d').date(),
                year=date_data['year'],
                month=date_data['month'],
                day=date_data['day'],
                quarter=date_data['quarter'],
                weekday=date_data['weekday']
            )
            session.add(date_dim)
        
        # Commit dimensions first
        session.commit()
        
        # Load sales facts
        print("💰 Loading sales transactions...")
        for i, sale_data in enumerate(data['sales'][:10]):  # Load first 10 sales
            if sale_data['user_id'] <= 5:  # Only for users we loaded
                sale = FactSales(
                    sale_id=i + 1,  # Use sequential IDs to avoid conflicts
                    user_id=sale_data['user_id'],
                    product_id=sale_data['product_id'],
                    date_id=20240101,  # Use our sample date
                    amount=sale_data['amount'],
                    quantity=sale_data['quantity'],
                    total_amount=sale_data['total_amount']
                )
                session.add(sale)
        
        # Commit all changes
        session.commit()
        session.close()
        
        print("✅ Sample data loaded successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error loading sample data: {e}")
        if 'session' in locals():
            session.rollback()
            session.close()
        return False

def verify_data():
    """Verify that data was loaded correctly"""
    print("\n🔍 Verifying data warehouse...")
    
    try:
        session = get_database_session()
        
        # Count records in each table
        user_count = session.query(DimUser).count()
        product_count = session.query(DimProduct).count()
        date_count = session.query(DimDate).count()
        sales_count = session.query(FactSales).count()
        
        print(f"📊 Data warehouse summary:")
        print(f"   👥 Users: {user_count}")
        print(f"   📦 Products: {product_count}")
        print(f"   📅 Dates: {date_count}")
        print(f"   💰 Sales: {sales_count}")
        
        # Show sample query
        print(f"\n💡 Sample analytics query:")
        from sqlalchemy import text
        result = session.execute(text("""
            SELECT u.name, SUM(s.total_amount) as total_spent
            FROM fact_sales s
            JOIN dim_users u ON s.user_id = u.user_id
            GROUP BY u.name
            ORDER BY total_spent DESC
            LIMIT 3
        """)).fetchall()
        
        print("   Top customers by spending:")
        for row in result:
            print(f"   💵 {row[0]}: ${row[1]:.2f}")
        
        session.close()
        return True
        
    except Exception as e:
        print(f"❌ Error verifying data: {e}")
        return False

def main():
    """Main application entry point"""
    print("🚀 Data Warehouse Project")
    print("=" * 50)
    
    # Step 1: Setup database
    if not setup_database():
        print("❌ Failed to setup database. Exiting.")
        return
    
    # Step 2: Load sample data
    if not load_sample_data():
        print("❌ Failed to load sample data. Exiting.")
        return
    
    # Step 3: Verify data
    if not verify_data():
        print("❌ Failed to verify data. Exiting.")
        return
    
    print("\n🎉 Data Warehouse is ready!")
    print("📈 Next steps:")
    print("   1. View data in pgAdmin: http://localhost:8080")
    print("   2. Run dashboard: python dashboard.py")
    print("   3. Run custom analytics queries")

if __name__ == "__main__":
    main()