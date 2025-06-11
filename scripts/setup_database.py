#!/usr/bin/env python3
"""
Database Setup Script
Creates all tables in the data warehouse
"""
import sys
import os

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config.database import engine, create_tables
from src.models.warehouse import Base, DimUser, DimProduct, DimDate, FactSales, StagingUsers, StagingSales
from sqlalchemy import text

def setup_database():
    """Set up the complete data warehouse schema"""
    print("🚀 Setting up Data Warehouse Database...")
    print("=" * 50)
    
    try:
        # Test connection
        print("📡 Testing database connection...")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"✅ Connected to: {version}")
        
        # Create all tables
        print("\n🏗️ Creating database tables...")
        Base.metadata.create_all(bind=engine)
        
        # List created tables
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """))
            tables = [row[0] for row in result.fetchall()]
            
            print("\n✅ Created tables:")
            for table in tables:
                print(f"   📋 {table}")
        
        print(f"\n🎉 Database setup completed successfully!")
        print(f"📊 Total tables: {len(tables)}")
        print("\n🔗 Database connection details:")
        print("   Host: localhost:5432")
        print("   Database: data_warehouse")
        print("   User: postgres")
        print("\n🌐 Access pgAdmin: http://localhost:8080")
        
    except Exception as e:
        print(f"❌ Error setting up database: {e}")
        return False
    
    return True

if __name__ == "__main__":
    setup_database()