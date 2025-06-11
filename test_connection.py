import os 
from sqlalchemy import create_engine, text 
from dotenv import load_dotenv 
 
load_dotenv() 
DATABASE_URL = "postgresql://postgres:password@localhost:5432/data_warehouse" 
engine = create_engine(DATABASE_URL) 
with engine.connect() as conn: 
    result = conn.execute(text("SELECT 'Hello Data Warehouse!' as message")) 
    print(result.fetchone()) 
