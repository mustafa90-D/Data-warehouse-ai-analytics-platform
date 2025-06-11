import requests
import json
import random
from typing import List, Dict
import sys
import os

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

class APIExtractor:
    """Extract data from various APIs for the data warehouse"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'DataWarehouse/1.0'
        })
    
    def extract_users_from_jsonplaceholder(self) -> List[Dict]:
        """Extract user data from JSONPlaceholder API"""
        try:
            print("🔌 Extracting users from JSONPlaceholder API...")
            url = "https://jsonplaceholder.typicode.com/users"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            users = response.json()
            print(f"✅ Extracted {len(users)} users")
            return users
            
        except Exception as e:
            print(f"❌ Error extracting users: {e}")
            return []
    
    def extract_posts_from_jsonplaceholder(self) -> List[Dict]:
        """Extract post data from JSONPlaceholder API"""
        try:
            print("🔌 Extracting posts from JSONPlaceholder API...")
            url = "https://jsonplaceholder.typicode.com/posts"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            posts = response.json()
            print(f"✅ Extracted {len(posts)} posts")
            return posts
            
        except Exception as e:
            print(f"❌ Error extracting posts: {e}")
            return []
    
    def generate_sample_products(self) -> List[Dict]:
        """Generate sample product data"""
        print("🔌 Generating sample product data...")
        
        products = [
            {"id": 1, "name": "Laptop Pro", "category": "Electronics", "price": 1299.99},
            {"id": 2, "name": "Wireless Mouse", "category": "Electronics", "price": 29.99},
            {"id": 3, "name": "Office Chair", "category": "Furniture", "price": 199.99},
            {"id": 4, "name": "Coffee Mug", "category": "Kitchen", "price": 12.99},
            {"id": 5, "name": "Notebook", "category": "Stationery", "price": 5.99},
            {"id": 6, "name": "Monitor 4K", "category": "Electronics", "price": 299.99},
            {"id": 7, "name": "Desk Lamp", "category": "Furniture", "price": 45.99},
            {"id": 8, "name": "Keyboard Mechanical", "category": "Electronics", "price": 89.99},
            {"id": 9, "name": "Water Bottle", "category": "Kitchen", "price": 15.99},
            {"id": 10, "name": "Pen Set", "category": "Stationery", "price": 19.99}
        ]
        
        print(f"✅ Generated {len(products)} products")
        return products
    
    def generate_sample_sales(self, users: List[Dict], products: List[Dict]) -> List[Dict]:
        """Generate sample sales transactions"""
        print("🔌 Generating sample sales data...")
        
        sales = []
        for i in range(50):  # Generate 50 sample sales
            user = random.choice(users)
            product = random.choice(products)
            quantity = random.randint(1, 5)
            
            sale = {
                "sale_id": i + 1,
                "user_id": user['id'],
                "product_id": product['id'],
                "quantity": quantity,
                "amount": product['price'],
                "total_amount": product['price'] * quantity,
                "sale_date": f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
            }
            sales.append(sale)
        
        print(f"✅ Generated {len(sales)} sales transactions")
        return sales
    
    def extract_all_data(self) -> Dict:
        """Extract all data sources"""
        print("🚀 Starting data extraction...")
        print("=" * 40)
        
        data = {
            'users': self.extract_users_from_jsonplaceholder(),
            'posts': self.extract_posts_from_jsonplaceholder(),
            'products': self.generate_sample_products()
        }
        
        # Generate sales data based on users and products
        if data['users'] and data['products']:
            data['sales'] = self.generate_sample_sales(data['users'], data['products'])
        else:
            data['sales'] = []
        
        print(f"\n🎉 Data extraction completed!")
        print(f"📊 Summary:")
        print(f"   👥 Users: {len(data['users'])}")
        print(f"   📝 Posts: {len(data['posts'])}")
        print(f"   📦 Products: {len(data['products'])}")
        print(f"   💰 Sales: {len(data['sales'])}")
        
        return data

if __name__ == "__main__":
    extractor = APIExtractor()
    data = extractor.extract_all_data()
    print("\n✅ Data extraction test completed!")