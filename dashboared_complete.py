#!/usr/bin/env python3
"""
Real AI-Powered Data Warehouse Analytics Dashboard
Uses Ollama for intelligent natural language to SQL conversion
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import sqlite3
import json
import re
from datetime import datetime, date

try:
    import ollama
    OLLAMA_AVAILABLE = True
except ImportError:
    OLLAMA_AVAILABLE = False
    st.error("Ollama not installed. Run: pip install ollama")

# Configure page
st.set_page_config(
    page_title="AI Analytics Dashboard",
    page_icon="chart",
    layout="wide"
)

@st.cache_resource
def create_sample_database():
    """Create sample SQLite database with your data warehouse structure"""
    
    # Create in-memory database
    conn = sqlite3.connect(':memory:', check_same_thread=False)
    
    try:
        # Create tables
        cursor = conn.cursor()
        
        # Create dimension tables
        cursor.execute('''
            CREATE TABLE dim_users (
                user_id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                city TEXT,
                phone TEXT,
                company TEXT,
                created_at DATETIME
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE dim_products (
                product_id INTEGER PRIMARY KEY,
                product_name TEXT,
                category TEXT,
                price REAL,
                description TEXT,
                created_at DATETIME
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE dim_date (
                date_id INTEGER PRIMARY KEY,
                date DATE,
                year INTEGER,
                month INTEGER,
                day INTEGER,
                quarter INTEGER,
                weekday TEXT,
                month_name TEXT,
                is_weekend TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE fact_sales (
                sale_id INTEGER PRIMARY KEY,
                user_id INTEGER,
                product_id INTEGER,
                date_id INTEGER,
                amount REAL,
                quantity INTEGER,
                total_amount REAL,
                created_at DATETIME,
                FOREIGN KEY (user_id) REFERENCES dim_users(user_id),
                FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
                FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
            )
        ''')
        
        # Insert sample data
        users_data = [
            (1, 'Leanne Graham', 'Sincere@april.biz', 'Gwenborough', '1-770-736-8031 x56442', 'Romaguera-Crona', datetime.now()),
            (2, 'Ervin Howell', 'Shanna@melissa.tv', 'Wisokyburgh', '010-692-6593 x09125', 'Deckow-Crist', datetime.now()),
            (3, 'Clementine Bauch', 'Nathan@yesenia.net', 'McKenziehaven', '1-463-123-4447', 'Romaguera-Jacobson', datetime.now()),
            (4, 'Patricia Lebsack', 'Julianne.OConner@kory.org', 'South Elvis', '493-170-9623 x156', 'Robel-Corkery', datetime.now()),
            (5, 'Chelsey Dietrich', 'Lucio_Hettinger@annie.ca', 'Roscoeview', '(254)954-1289', 'Keebler LLC', datetime.now())
        ]
        
        cursor.executemany('INSERT INTO dim_users VALUES (?, ?, ?, ?, ?, ?, ?)', users_data)
        
        products_data = [
            (1, 'Laptop Pro', 'Electronics', 1299.99, 'High-performance laptop', datetime.now()),
            (2, 'Wireless Mouse', 'Electronics', 29.99, 'Ergonomic wireless mouse', datetime.now()),
            (3, 'Office Chair', 'Furniture', 199.99, 'Comfortable office chair', datetime.now()),
            (4, 'Coffee Mug', 'Kitchen', 12.99, 'Ceramic coffee mug', datetime.now()),
            (5, 'Notebook', 'Office', 5.99, 'Spiral notebook', datetime.now()),
            (6, 'Desk Lamp', 'Furniture', 45.99, 'LED desk lamp', datetime.now()),
            (7, 'Keyboard', 'Electronics', 79.99, 'Mechanical keyboard', datetime.now()),
            (8, 'Water Bottle', 'Kitchen', 19.99, 'Stainless steel water bottle', datetime.now()),
            (9, 'Monitor Stand', 'Furniture', 39.99, 'Adjustable monitor stand', datetime.now()),
            (10, 'Phone Case', 'Electronics', 24.99, 'Protective phone case', datetime.now())
        ]
        
        cursor.executemany('INSERT INTO dim_products VALUES (?, ?, ?, ?, ?, ?)', products_data)
        
        date_data = [
            (20240101, date(2024, 1, 1), 2024, 1, 1, 1, 'Monday', 'January', 'No'),
            (20240115, date(2024, 1, 15), 2024, 1, 15, 1, 'Monday', 'January', 'No'),
            (20240201, date(2024, 2, 1), 2024, 2, 1, 1, 'Thursday', 'February', 'No'),
            (20240315, date(2024, 3, 15), 2024, 3, 15, 1, 'Friday', 'March', 'No'),
            (20240420, date(2024, 4, 20), 2024, 4, 20, 2, 'Saturday', 'April', 'Yes')
        ]
        
        cursor.executemany('INSERT INTO dim_date VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', date_data)
        
        sales_data = [
            (1, 2, 1, 20240101, 1299.99, 2, 2599.98, datetime.now()),
            (2, 2, 7, 20240115, 79.99, 1, 79.99, datetime.now()),     
            (3, 2, 6, 20240201, 45.99, 5, 229.95, datetime.now()),    
            (4, 2, 2, 20240315, 29.99, 3, 89.97, datetime.now()),     
            (5, 5, 4, 20240420, 12.99, 1, 12.99, datetime.now()),     
            (6, 1, 8, 20240420, 19.99, 1, 19.99, datetime.now())      
        ]
        
        cursor.executemany('INSERT INTO fact_sales VALUES (?, ?, ?, ?, ?, ?, ?, ?)', sales_data)
        
        conn.commit()
        return conn
        
    except Exception as e:
        st.error(f"Error creating sample database: {e}")
        return None

class RealAnalyticsAI:
    """Real AI-powered analytics assistant using Ollama"""
    
    def __init__(self):
        self.database_schema = """
        Database Schema - Star Schema Data Warehouse:
        
        DIMENSION TABLES:
        1. dim_users (user_id, name, email, city, phone, company, created_at)
        2. dim_products (product_id, product_name, category, price, description, created_at)
        3. dim_date (date_id, date, year, month, day, quarter, weekday, month_name, is_weekend)
        
        FACT TABLE:
        4. fact_sales (sale_id, user_id, product_id, date_id, amount, quantity, total_amount, created_at)
        
        AVAILABLE PRODUCTS: Laptop Pro, Wireless Mouse, Office Chair, Coffee Mug, Notebook, Desk Lamp, Keyboard, Water Bottle, Monitor Stand, Phone Case
        CATEGORIES: Electronics, Furniture, Kitchen, Office
        CUSTOMERS: Leanne Graham, Ervin Howell, Clementine Bauch, Patricia Lebsack, Chelsey Dietrich
        """
        
        self.system_prompt = """You are an expert data analyst and SQL specialist. Your role is to:

1. UNDERSTAND business questions and convert them to precise SQL queries
2. ANALYZE data patterns and provide actionable business insights
3. IDENTIFY risks, opportunities, and trends in the data
4. GENERATE professional recommendations based on analysis

CRITICAL RULES:
- Always use proper JOINs between fact_sales and dimension tables
- Use meaningful column aliases for clarity
- Group and order results appropriately
- Return only valid SQLite syntax
- For specific products: use LIKE '%ProductName%' for flexible matching
- For customer analysis: always include city and company context
- For revenue analysis: use SUM(total_amount) for accurate totals

RESPONSE FORMAT:
1. First: Generate the SQL query only
2. Then: Provide data-driven business insights"""

    def get_available_models(self):
        """Get list of available Ollama models"""
        try:
            models = ollama.list()
            return [model['name'] for model in models['models']]
        except:
            return ['llama2', 'mistral', 'codellama']

    def generate_sql(self, question: str, model: str = 'mistral') -> str:
        """Generate SQL query using Ollama AI"""
        
        # Check for comprehensive analysis request
        if any(phrase in question.lower() for phrase in ['complete dashboard', 'entire dashboard', 'everything', 'complete analysis']):
            return "COMPREHENSIVE_ANALYSIS"
        
        if not OLLAMA_AVAILABLE:
            return self._fallback_sql(question)
        
        try:
            prompt = f"""
{self.system_prompt}

Database Schema:
{self.database_schema}

CRITICAL SCHEMA RULES:
- fact_sales contains: sale_id, user_id, product_id, date_id, amount, quantity, total_amount, created_at
- dim_products contains: product_id, product_name, category, price, description, created_at  
- dim_users contains: user_id, name, email, city, phone, company, created_at
- ALWAYS use JOINs: fact_sales s JOIN dim_products p ON s.product_id = p.product_id
- product_name is ONLY in dim_products, NOT in fact_sales
- For product queries: WHERE p.product_name LIKE '%ProductName%'

Business Question: "{question}"

IMPORTANT RULES:
- For product-specific questions, use WHERE p.product_name LIKE '%ProductName%'
- ALWAYS JOIN tables properly - never assume columns exist in fact_sales
- Do NOT make assumptions about cities or geography unless explicitly asked
- Focus on the exact data requested, not creative interpretations

Generate a precise SQL query to answer this question. Return ONLY the SQL query without explanations.
"""
            
            response = ollama.generate(
                model=model,
                prompt=prompt,
                options={
                    'temperature': 0.1,  # Low temperature for consistent SQL
                    'top_p': 0.9,
                    'num_predict': 200
                }
            )
            
            sql_query = response['response'].strip()
            
            # Clean up the response to extract just the SQL
            sql_query = self._extract_sql_from_response(sql_query)
            
            # Validate the SQL
            if self._validate_sql(sql_query):
                return sql_query
            else:
                return self._fallback_sql(question)
                
        except Exception as e:
            st.warning(f"AI model error: {e}. Using fallback SQL generation.")
            return self._fallback_sql(question)

    def _extract_sql_from_response(self, response: str) -> str:
        """Extract SQL query from AI response"""
        
        # Remove markdown code blocks
        response = re.sub(r'```sql\s*\n', '', response)
        response = re.sub(r'```\s*$', '', response)
        response = re.sub(r'```', '', response)
        
        # Find SQL keywords to identify the query
        sql_start = -1
        for keyword in ['SELECT', 'WITH', 'select', 'with']:
            pos = response.find(keyword)
            if pos != -1 and (sql_start == -1 or pos < sql_start):
                sql_start = pos
        
        if sql_start != -1:
            response = response[sql_start:]
        
        # Clean up extra text after semicolon
        if ';' in response:
            response = response.split(';')[0] + ';'
        
        return response.strip()

    def _validate_sql(self, sql_query: str) -> bool:
        """Basic SQL validation"""
        sql_lower = sql_query.lower()
        
        # Check for required elements
        required_elements = ['select', 'from']
        has_required = all(element in sql_lower for element in required_elements)
        
        # Check for dangerous operations (security)
        dangerous_keywords = ['drop', 'delete', 'update', 'insert', 'alter', 'create']
        is_safe = not any(keyword in sql_lower for keyword in dangerous_keywords)
        
        # Check for schema errors
        schema_errors = [
            'fact_sales.product_name',  # product_name not in fact_sales
            'fact_sales.name',          # name not in fact_sales
            'fact_sales.category',      # category not in fact_sales
        ]
        has_schema_errors = any(error in sql_lower for error in schema_errors)
        
        return has_required and is_safe and not has_schema_errors

    def _fallback_sql(self, question: str) -> str:
        """Fallback SQL generation when AI fails"""
        question_lower = question.lower()
        
        # Enhanced fallback patterns
        if any(word in question_lower for word in ['top customer', 'best customer', 'biggest customer']):
            return """
                SELECT u.name, u.city, u.company, SUM(s.total_amount) as total_spent
                FROM fact_sales s
                JOIN dim_users u ON s.user_id = u.user_id
                GROUP BY u.user_id, u.name, u.city, u.company
                ORDER BY total_spent DESC
                LIMIT 10
            """
        elif 'product performance' in question_lower or 'revenue by product' in question_lower:
            return """
                SELECT p.product_name, p.category, p.price,
                       COUNT(s.sale_id) as sales_count,
                       SUM(s.quantity) as total_quantity_sold,
                       SUM(s.total_amount) as total_revenue
                FROM fact_sales s
                JOIN dim_products p ON s.product_id = p.product_id
                GROUP BY p.product_id, p.product_name, p.category, p.price
                ORDER BY total_revenue DESC
            """
        elif any(product in question_lower for product in ['laptop', 'mouse', 'chair', 'mug', 'lamp', 'keyboard']):
            # Extract product name
            for product in ['laptop', 'mouse', 'chair', 'mug', 'lamp', 'keyboard', 'bottle', 'stand', 'case']:
                if product in question_lower:
                    return f"""
                        SELECT p.product_name, p.category, p.price,
                               SUM(s.quantity) as quantity_sold,
                               SUM(s.total_amount) as total_revenue,
                               COUNT(s.sale_id) as order_count,
                               u.name as customer
                        FROM fact_sales s
                        JOIN dim_products p ON s.product_id = p.product_id
                        JOIN dim_users u ON s.user_id = u.user_id
                        WHERE p.product_name LIKE '%{product}%'
                        GROUP BY p.product_id, p.product_name, p.category, p.price, u.name
                        ORDER BY total_revenue DESC
                    """
        else:
            return """
                SELECT 
                    COUNT(*) as total_sales,
                    SUM(s.total_amount) as total_revenue,
                    AVG(s.total_amount) as avg_order_value,
                    COUNT(DISTINCT s.user_id) as unique_customers
                FROM fact_sales s
            """

    def generate_insights(self, data: pd.DataFrame, question: str, sql_query: str, model: str = 'mistral') -> list:
        """Generate intelligent business insights using AI"""
        
        if data.empty:
            return ["No data found for your query."]
        
        if not OLLAMA_AVAILABLE:
            return self._fallback_insights(data, question)
        
        try:
            # Prepare data summary for AI
            data_summary = self._prepare_data_summary(data)
            
            prompt = f"""
You are a senior business analyst. Analyze this data and provide actionable insights.

Original Question: "{question}"
SQL Query Used: {sql_query}
Data Results:
{data_summary}

Provide 3-5 specific business insights in this format:
- INSIGHT: [insight description]
- RECOMMENDATION: [actionable recommendation]
- RISK/OPPORTUNITY: [business implications]

Focus on:
1. Revenue concentration and risks
2. Product performance patterns
3. Customer behavior insights
4. Market opportunities
5. Operational recommendations

Return insights as bullet points without formatting.
"""
            
            response = ollama.generate(
                model=model,
                prompt=prompt,
                options={
                    'temperature': 0.3,  # Slightly creative for insights
                    'top_p': 0.9,
                    'num_predict': 300
                }
            )
            
            insights_text = response['response'].strip()
            insights = [line.strip() for line in insights_text.split('\n') if line.strip()]
            
            return insights if insights else self._fallback_insights(data, question)
            
        except Exception as e:
            st.warning(f"AI insights error: {e}. Using analytical insights.")
            return self._fallback_insights(data, question)

    def _prepare_data_summary(self, data: pd.DataFrame) -> str:
        """Prepare data summary for AI analysis"""
        summary = f"Dataset: {len(data)} rows\n"
        summary += f"Columns: {', '.join(data.columns)}\n\n"
        
        # Add top 5 rows
        summary += "Top Results:\n"
        for idx, row in data.head(5).iterrows():
            row_summary = ", ".join([f"{col}: {val}" for col, val in row.items()])
            summary += f"- {row_summary}\n"
        
        # Add aggregations if relevant
        numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
        if len(numeric_columns) > 0:
            summary += "\nKey Metrics:\n"
            for col in numeric_columns:
                if 'revenue' in col.lower() or 'amount' in col.lower() or 'spent' in col.lower():
                    total = data[col].sum()
                    summary += f"- Total {col}: ${total:,.2f}\n"
                elif 'quantity' in col.lower() or 'count' in col.lower():
                    total = data[col].sum()
                    summary += f"- Total {col}: {total:,}\n"
        
        return summary

    def _fallback_insights(self, data: pd.DataFrame, question: str) -> list:
        """Fallback insights when AI is unavailable"""
        insights = []
        
        # Revenue concentration analysis
        if 'total_spent' in data.columns and len(data) > 0:
            total_revenue = data['total_spent'].sum()
            top_customer_revenue = data['total_spent'].iloc[0]
            concentration = (top_customer_revenue / total_revenue * 100) if total_revenue > 0 else 0
            
            insights.append(f"REVENUE CONCENTRATION: Top customer represents {concentration:.1f}% of total revenue")
            
            if concentration > 80:
                insights.append("CRITICAL RISK: Extreme customer dependency detected")
                insights.append("RECOMMENDATION: Immediate customer diversification strategy needed")
            elif concentration > 50:
                insights.append("MODERATE RISK: High customer concentration")
                insights.append("RECOMMENDATION: Develop additional customer acquisition channels")
        
        # Product performance analysis
        elif 'total_revenue' in data.columns and 'product_name' in data.columns:
            top_product = data.iloc[0]
            total_revenue = data['total_revenue'].sum()
            product_share = (top_product['total_revenue'] / total_revenue * 100) if total_revenue > 0 else 0
            
            insights.append(f"TOP PERFORMER: {top_product['product_name']} generates {product_share:.1f}% of product revenue")
            insights.append(f"REVENUE IMPACT: ${top_product['total_revenue']:,.2f} from this product line")
            
            if product_share > 80:
                insights.append("RISK: Over-dependence on single product")
                insights.append("RECOMMENDATION: Diversify product portfolio")
        
        # Business summary insights
        elif 'total_revenue' in data.columns and len(data) == 1:
            row = data.iloc[0]
            avg_order = row.get('avg_order_value', 0)
            customers = row.get('unique_customers', 0)
            
            insights.append(f"BUSINESS HEALTH: {customers} active customers")
            insights.append(f"ORDER VALUE: Average order ${avg_order:.2f}")
            
            if avg_order > 500:
                insights.append("OPPORTUNITY: High-value customer base")
            if customers < 10:
                insights.append("FOCUS AREA: Customer acquisition needed")
        
        return insights if insights else ["Analysis complete. Data processed successfully."]

    def generate_comprehensive_analysis(self, conn, model: str = 'mistral') -> dict:
        """Generate comprehensive dashboard analysis by running multiple queries"""
        
        # Define comprehensive queries
        analysis_queries = {
            "business_summary": """
                SELECT 
                    COUNT(*) as total_sales,
                    SUM(s.total_amount) as total_revenue,
                    AVG(s.total_amount) as avg_order_value,
                    COUNT(DISTINCT s.user_id) as unique_customers,
                    COUNT(DISTINCT p.product_id) as unique_products
                FROM fact_sales s
                JOIN dim_products p ON s.product_id = p.product_id
            """,
            "product_performance": """
                SELECT p.product_name, p.category, p.price,
                       COUNT(s.sale_id) as sales_count,
                       SUM(s.quantity) as total_quantity_sold,
                       SUM(s.total_amount) as total_revenue,
                       ROUND((SUM(s.total_amount) * 100.0 / (SELECT SUM(total_amount) FROM fact_sales)), 2) as revenue_percentage
                FROM fact_sales s
                JOIN dim_products p ON s.product_id = p.product_id
                GROUP BY p.product_id, p.product_name, p.category, p.price
                ORDER BY total_revenue DESC
            """,
            "customer_analysis": """
                SELECT u.name, u.city, u.company,
                       COUNT(s.sale_id) as order_count,
                       SUM(s.total_amount) as total_spent,
                       AVG(s.total_amount) as avg_order_value,
                       ROUND((SUM(s.total_amount) * 100.0 / (SELECT SUM(total_amount) FROM fact_sales)), 2) as revenue_percentage
                FROM fact_sales s
                JOIN dim_users u ON s.user_id = u.user_id
                GROUP BY u.user_id, u.name, u.city, u.company
                ORDER BY total_spent DESC
            """,
            "category_breakdown": """
                SELECT p.category,
                       COUNT(DISTINCT p.product_id) as product_count,
                       COUNT(s.sale_id) as sales_count,
                       SUM(s.total_amount) as total_revenue,
                       AVG(s.total_amount) as avg_order_value,
                       ROUND((SUM(s.total_amount) * 100.0 / (SELECT SUM(total_amount) FROM fact_sales)), 2) as revenue_percentage
                FROM fact_sales s
                JOIN dim_products p ON s.product_id = p.product_id
                GROUP BY p.category
                ORDER BY total_revenue DESC
            """
        }
        
        # Execute all queries
        results = {}
        for analysis_type, query in analysis_queries.items():
            try:
                results[analysis_type] = pd.read_sql_query(query, conn)
            except Exception as e:
                st.warning(f"Error in {analysis_type} analysis: {e}")
                results[analysis_type] = pd.DataFrame()
        
        return results

def execute_query(sql_query: str, conn):
    """Execute SQL query safely and return results"""
    try:
        result = pd.read_sql_query(sql_query, conn)
        return result
    except Exception as e:
        error_msg = str(e).lower()
        st.error(f"Query execution error: {e}")
        
        # Provide helpful debugging info
        if "no such column" in error_msg:
            st.warning("Schema Error: The AI generated SQL with incorrect column references. Using fallback query.")
            if "product_name" in error_msg and "fact_sales" in sql_query.lower():
                st.info("Tip: product_name is in dim_products table, not fact_sales. Proper JOIN needed.")
        
        return pd.DataFrame()

def create_automatic_charts(conn):
    """Create automatic business charts"""
    
    sales_with_products = execute_query("""
        SELECT s.*, p.product_name, p.category, u.name as customer_name
        FROM fact_sales s
        JOIN dim_products p ON s.product_id = p.product_id
        JOIN dim_users u ON s.user_id = u.user_id
    """, conn)
    
    if sales_with_products.empty:
        st.warning("No sales data available for charts")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Revenue by Product
        product_revenue = sales_with_products.groupby('product_name')['total_amount'].sum().reset_index()
        product_revenue = product_revenue.sort_values('total_amount', ascending=False)
        
        fig1 = px.bar(
            product_revenue,
            x='product_name',
            y='total_amount',
            title="Revenue by Product",
            labels={'total_amount': 'Revenue ($)', 'product_name': 'Product'},
            color='total_amount',
            color_continuous_scale='Blues'
        )
        fig1.update_layout(xaxis_tickangle=45, height=400)
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # Revenue by Customer
        customer_revenue = sales_with_products.groupby('customer_name')['total_amount'].sum().reset_index()
        customer_revenue = customer_revenue.sort_values('total_amount', ascending=False)
        
        fig2 = px.bar(
            customer_revenue,
            x='customer_name',
            y='total_amount',
            title="Revenue by Customer",
            labels={'total_amount': 'Revenue ($)', 'customer_name': 'Customer'},
            color='total_amount',
            color_continuous_scale='Greens'
        )
        fig2.update_layout(xaxis_tickangle=45, height=400)
        st.plotly_chart(fig2, use_container_width=True)
    
    col3, col4 = st.columns(2)
    
    with col3:
        # Revenue by Category
        category_revenue = sales_with_products.groupby('category')['total_amount'].sum().reset_index()
        
        fig3 = px.pie(
            category_revenue,
            values='total_amount',
            names='category',
            title="Revenue by Category"
        )
        fig3.update_layout(height=400)
        st.plotly_chart(fig3, use_container_width=True)
    
    with col4:
        # Customer Revenue Share
        customer_pie = sales_with_products.groupby('customer_name')['total_amount'].sum().reset_index()
        
        fig4 = px.pie(
            customer_pie,
            values='total_amount',
            names='customer_name',
            title="Customer Revenue Share"
        )
        fig4.update_layout(height=400)
        st.plotly_chart(fig4, use_container_width=True)

def main():
    st.markdown("# Real AI Analytics Dashboard")
    st.markdown("## Intelligent Business Intelligence Platform")
    
    # Database setup
    conn = create_sample_database()
    if conn is None:
        st.error("Could not create data warehouse.")
        st.stop()
    else:
        st.success("Connected to data warehouse. AI analytics ready.")
    
    # Initialize AI assistant
    ai_assistant = RealAnalyticsAI()
    
    # Sidebar configuration
    st.sidebar.header("AI Configuration")
    
    if OLLAMA_AVAILABLE:
        available_models = ai_assistant.get_available_models()
        selected_model = st.sidebar.selectbox(
            "AI Model:",
            available_models,
            index=0 if available_models else 0
        )
        st.sidebar.success(f"AI Model: {selected_model}")
    else:
        st.sidebar.warning("Ollama not available. Using fallback analysis.")
        selected_model = "fallback"
    
    st.sidebar.markdown("---")
    st.sidebar.header("Ask Your Data")
    
    # Quick questions
    quick_questions = [
        "Give me a complete dashboard analysis of everything",
        "Who are my top customers by revenue?",
        "Show me product performance analysis",
        "What is the revenue for Coffee Mug?",
        "Give me customer analysis with locations",
        "Which Electronics products sell best?",
        "Show me sales summary and trends",
        "Who bought Laptop Pro and how much?",
        "Compare Electronics vs Furniture revenue"
    ]
    
    selected_question = st.sidebar.selectbox("Quick Questions:", [""] + quick_questions)
    custom_question = st.sidebar.text_input("Custom Question:")
    
    question = custom_question if custom_question else selected_question
    
    if question:
        st.markdown(f"## Analysis: {question}")
        
        with st.spinner(f"AI is analyzing: {question}"):
            # Generate SQL with AI
            sql_query = ai_assistant.generate_sql(question, selected_model)
            
            # Check for comprehensive analysis
            if sql_query == "COMPREHENSIVE_ANALYSIS":
                st.markdown("### Comprehensive Dashboard Analysis")
                
                with st.spinner("Running complete business analysis..."):
                    # Get comprehensive data
                    comprehensive_data = ai_assistant.generate_comprehensive_analysis(conn, selected_model)
                    
                    # Prepare comprehensive summary for AI
                    comprehensive_summary = "COMPLETE BUSINESS DASHBOARD ANALYSIS:\n\n"
                    
                    for analysis_type, data in comprehensive_data.items():
                        if not data.empty:
                            comprehensive_summary += f"\n{analysis_type.upper()}:\n"
                            comprehensive_summary += data.to_string(index=False) + "\n"
                    
                    # Generate AI insights for everything
                    if not OLLAMA_AVAILABLE:
                        insights = ["Comprehensive analysis requires AI model. Using analytical summary:", 
                                  "Business shows high concentration risk across products and customers.",
                                  "Electronics dominates revenue with significant dependency on premium products.",
                                  "Customer base is small with concentration risk.",
                                  "Diversification opportunities exist in Kitchen and Furniture categories."]
                    else:
                        try:
                            prompt = f"""
You are a senior business consultant analyzing a complete business dashboard. Provide a comprehensive strategic analysis.

{comprehensive_summary}

Provide a complete executive summary covering:
1. Overall business health and performance
2. Critical risks and vulnerabilities  
3. Market opportunities and strengths
4. Strategic recommendations for growth
5. Operational improvements needed

Format as executive bullet points without markdown formatting.
"""
                            
                            response = ollama.generate(
                                model=selected_model,
                                prompt=prompt,
                                options={
                                    'temperature': 0.3,
                                    'top_p': 0.9,
                                    'num_predict': 500
                                }
                            )
                            
                            insights_text = response['response'].strip()
                            insights = [line.strip() for line in insights_text.split('\n') if line.strip()]
                            
                        except Exception as e:
                            insights = [f"AI analysis error: {e}", "Using analytical insights based on data patterns."]
                    
                    # Display comprehensive results
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.markdown("### Complete Business Overview")
                        
                        # Show key tables
                        for analysis_type, data in comprehensive_data.items():
                            if not data.empty:
                                st.markdown(f"**{analysis_type.replace('_', ' ').title()}:**")
                                
                                # Format display data
                                display_data = data.copy()
                                for col in display_data.columns:
                                    if any(keyword in col.lower() for keyword in ['spent', 'revenue', 'amount', 'price']):
                                        if display_data[col].dtype in ['float64', 'int64']:
                                            display_data[col] = display_data[col].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else "")
                                
                                st.dataframe(display_data, use_container_width=True)
                                st.markdown("---")
                    
                    with col2:
                        st.markdown("### Executive Summary")
                        for insight in insights:
                            if insight:
                                st.markdown(f"• {insight}")
                    
                    # Show comprehensive query information
                    with st.expander("View Analysis Methodology"):
                        st.markdown("**Comprehensive analysis includes:**")
                        st.markdown("• Business performance summary")
                        st.markdown("• Product performance ranking") 
                        st.markdown("• Customer analysis with geography")
                        st.markdown("• Category breakdown and opportunities")
                        st.markdown("• Risk assessment and recommendations")
            
            else:
                # Regular single query analysis
                result_data = execute_query(sql_query, conn)
                
                if not result_data.empty:
                    col1, col2 = st.columns([2, 1])
                    
                    with col1:
                        st.markdown("### Results")
                        
                        # Format display data
                        display_data = result_data.copy()
                        for col in display_data.columns:
                            if any(keyword in col.lower() for keyword in ['spent', 'revenue', 'amount', 'price']):
                                if display_data[col].dtype in ['float64', 'int64']:
                                    display_data[col] = display_data[col].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else "")
                        
                        st.dataframe(display_data, use_container_width=True)
                    
                    with col2:
                        st.markdown("### AI Insights")
                        with st.spinner("Generating insights..."):
                            insights = ai_assistant.generate_insights(result_data, question, sql_query, selected_model)
                            for insight in insights:
                                st.markdown(f"• {insight}")
                    
                    # Show SQL query
                    with st.expander("View Generated SQL Query"):
                        st.code(sql_query, language='sql')
                        
                else:
                    st.warning("No data found. The AI may need a different approach.")
                    with st.expander("Debug: View Generated SQL"):
                        st.code(sql_query, language='sql')
    
    # Automatic Charts
    st.markdown("---")
    st.markdown("## Business Intelligence Dashboard")
    
    create_automatic_charts(conn)
    
    # Key Metrics
    st.markdown("---")
    st.markdown("## Key Performance Indicators")
    
    try:
        users_count = execute_query("SELECT COUNT(*) as count FROM dim_users", conn).iloc[0]['count']
        products_count = execute_query("SELECT COUNT(*) as count FROM dim_products", conn).iloc[0]['count']
        sales_count = execute_query("SELECT COUNT(*) as count FROM fact_sales", conn).iloc[0]['count']
        total_revenue = execute_query("SELECT SUM(total_amount) as total FROM fact_sales", conn).iloc[0]['total']
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Customers", users_count)
        with col2:
            st.metric("Product Catalog", products_count)
        with col3:
            st.metric("Total Sales", sales_count)
        with col4:
            st.metric("Revenue", f"${total_revenue:,.2f}")
            
        # Transaction History
        st.markdown("---")
        st.markdown("## Recent Transactions")
        
        recent_sales = execute_query("""
            SELECT 
                s.sale_id,
                u.name as customer,
                p.product_name,
                s.quantity,
                s.total_amount,
                date(s.created_at) as sale_date
            FROM fact_sales s
            JOIN dim_users u ON s.user_id = u.user_id
            JOIN dim_products p ON s.product_id = p.product_id
            ORDER BY s.created_at DESC
        """, conn)
        
        if not recent_sales.empty:
            display_sales = recent_sales.copy()
            display_sales['total_amount'] = display_sales['total_amount'].apply(lambda x: f"${x:,.2f}")
            st.dataframe(display_sales, use_container_width=True, hide_index=True)
            
    except Exception as e:
        st.error(f"Error loading metrics: {e}")

if __name__ == "__main__":
    main()