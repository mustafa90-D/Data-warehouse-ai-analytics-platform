#!/usr/bin/env python3
"""
Reset Database Script
Drops all existing tables and recreates them with updated schema
"""
import sys
import os

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from config.database import engine, Base
from src.models.warehouse import DimUser, DimProduct, DimDate, FactSales, StagingUsers, StagingSales
from sqlalchemy import text

def reset_database():
    """Drop all tables and recreate them"""
    print("🔄 Resetting Data Warehouse Database...")
    print("=" * 50)
    
    try:
        # Test connection
        print("📡 Testing database connection...")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"✅ Connected to: {version}")
        
        # Drop all tables
        print("\n🗑️ Dropping existing tables...")
        Base.metadata.drop_all(bind=engine)
        print("✅ All tables dropped")
        
        # Create all tables with new schema
        print("\n🏗️ Creating tables with updated schema...")
        Base.metadata.create_all(bind=engine)
        
        # List created tables
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name, 
                       (SELECT string_agg(column_name || ' ' || data_type || 
                                         CASE WHEN character_maximum_length IS NOT NULL 
                                              THEN '(' || character_maximum_length || ')' 
                                              ELSE '' END, ', ')
                        FROM information_schema.columns 
                        WHERE table_name = t.table_name 
                        AND table_schema = 'public') as columns
                FROM information_schema.tables t
                WHERE table_schema = 'public'
                ORDER BY table_name
            """))
            tables = result.fetchall()
            
            print("\n✅ Created tables with updated schema:")
            for table_name, columns in tables:
                print(f"   📋 {table_name}")
                if 'dim_users' == table_name:
                    print(f"      📝 Key fields: phone(50), company(200), name(200)")
        
        print(f"\n🎉 Database reset completed successfully!")
        print(f"📊 Total tables: {len(tables)}")
        print("\n🔗 Ready for data loading!")
        
    except Exception as e:
        print(f"❌ Error resetting database: {e}")
        return False
    
    return True

if __name__ == "__main__":
    reset_database()