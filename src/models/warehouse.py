from sqlalchemy import Column, Integer, String, DateTime, Numeric, ForeignKey, Date, Text
from sqlalchemy.orm import relationship
from datetime import datetime
import sys
import os

# Add the project root to the path so we can import from config
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from config.database import Base

# Staging Tables (Raw Data Storage)
class StagingUsers(Base):
    __tablename__ = 'staging_users'
    
    id = Column(Integer, primary_key=True)
    raw_data = Column(Text)
    source = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)

class StagingSales(Base):
    __tablename__ = 'staging_sales'
    
    id = Column(Integer, primary_key=True)
    raw_data = Column(Text)
    source = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)

# Dimension Tables
class DimUser(Base):
    __tablename__ = 'dim_users'
    
    user_id = Column(Integer, primary_key=True)
    name = Column(String(200))
    email = Column(String(150))
    city = Column(String(100))
    phone = Column(String(50))  # Increased from 20 to 50
    company = Column(String(200))  # Increased from 100 to 200
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship to fact table
    sales = relationship("FactSales", back_populates="user")

class DimProduct(Base):
    __tablename__ = 'dim_products'
    
    product_id = Column(Integer, primary_key=True)
    product_name = Column(String(200))  # Increased from 100 to 200
    category = Column(String(100))  # Increased from 50 to 100
    price = Column(Numeric(10, 2))
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship to fact table
    sales = relationship("FactSales", back_populates="product")

class DimDate(Base):
    __tablename__ = 'dim_date'
    
    date_id = Column(Integer, primary_key=True)
    date = Column(Date)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    quarter = Column(Integer)
    weekday = Column(String(20))  # Increased from 10 to 20
    month_name = Column(String(20))  # Increased from 10 to 20
    is_weekend = Column(String(10))  # Increased from 5 to 10
    
    # Relationship to fact table
    sales = relationship("FactSales", back_populates="date")

# Fact Table (Business Transactions)
class FactSales(Base):
    __tablename__ = 'fact_sales'
    
    sale_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('dim_users.user_id'))
    product_id = Column(Integer, ForeignKey('dim_products.product_id'))
    date_id = Column(Integer, ForeignKey('dim_date.date_id'))
    amount = Column(Numeric(10, 2))
    quantity = Column(Integer)
    total_amount = Column(Numeric(10, 2))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships to dimension tables
    user = relationship("DimUser", back_populates="sales")
    product = relationship("DimProduct", back_populates="sales")
    date = relationship("DimDate", back_populates="sales")