#!/usr/bin/env python3
"""
AI-Enhanced Data Warehouse Dashboard
Adds conversational analytics to your existing system
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import openai
import sys
import os
from datetime import datetime

# Add the project root to the path
sys.path.append(os.path.dirname(__file__))

try:
    from config.database import get_database_session
    from sqlalchemy import text
except ImportError as e:
    st.error(f"Error importing modules: {e}")
    st.stop()

# Configure page
st.set_page_config(
    page_title="DataMilo - AI Analytics Assistant",
    page_icon="AI",
    layout="wide"
)

class DataMiloAI:
    """AI-powered analytics assistant for your data warehouse"""
    
    def __init__(self):
        # Initialize OpenAI (you'll need to add your API key)
        # openai.api_key = "your-openai-api-key"  # Add your key here
        pass
    
    def text_to_sql(self, question: str) -> str:
        """Convert natural language question to SQL query"""
        
        # Database schema context for AI
        schema_context = """
        You are a SQL expert working with a PostgreSQL data warehouse with these tables:
        
        1. dim_users (customers):
           - user_id (primary key)
           - name, email, city, phone, company
           
        2. dim_products (product catalog):
           - product_id (primary key) 
           - product_name, category, price
           
        3. dim_date (time dimension):
           - date_id (primary key, format YYYYMMDD)
           - date, year, month, day, quarter, weekday
           
        4. fact_sales (transactions):
           - sale_id (primary key)
           - user_id (foreign key to dim_users)
           - product_id (foreign key to dim_products)
           - date_id (foreign key to dim_date)
           - amount, quantity, total_amount
        
        Convert the following question to a SQL query. Return ONLY the SQL query, no explanations:
        """
        
        # Predefined SQL queries for common questions (fallback)
        query_mapping = {
            "top customers": """
                SELECT u.name, SUM(s.total_amount) as total_spent
                FROM fact_sales s
                JOIN dim_users u ON s.user_id = u.user_id
                GROUP BY u.user_id, u.name
                ORDER BY total_spent DESC
                LIMIT 5
            """,
            "revenue by product": """
                SELECT p.product_name, p.category, SUM(s.total_amount) as revenue
                FROM fact_sales s
                JOIN dim_products p ON s.product_id = p.product_id
                GROUP BY p.product_id, p.product_name, p.category
                ORDER BY revenue DESC
            """,
            "sales summary": """
                SELECT 
                    COUNT(*) as total_sales,
                    SUM(s.total_amount) as total_revenue,
                    AVG(s.total_amount) as avg_order_value,
                    COUNT(DISTINCT s.user_id) as unique_customers
                FROM fact_sales s
            """,
            "customer analysis": """
                SELECT 
                    u.name,
                    u.city,
                    u.company,
                    COUNT(s.sale_id) as order_count,
                    SUM(s.total_amount) as total_spent,
                    AVG(s.total_amount) as avg_order
                FROM fact_sales s
                JOIN dim_users u ON s.user_id = u.user_id
                GROUP BY u.user_id, u.name, u.city, u.company
                ORDER BY total_spent DESC
            """
        }
        
        # Simple keyword matching (you can enhance with OpenAI later)
        question_lower = question.lower()
        
        if "top customer" in question_lower or "best customer" in question_lower:
            return query_mapping["top customers"]
        elif "revenue by product" in question_lower or "product performance" in question_lower:
            return query_mapping["revenue by product"] 
        elif "customer analysis" in question_lower or "customer breakdown" in question_lower:
            return query_mapping["customer analysis"]
        else:
            return query_mapping["sales summary"]
    
    def generate_insights(self, data: pd.DataFrame, question: str) -> str:
        """Generate business insights from query results"""
        
        if data.empty:
            return "No data found for your query."
        
        insights = []
        
        # Revenue concentration analysis
        if 'total_spent' in data.columns:
            total_revenue = data['total_spent'].sum()
            top_customer_revenue = data['total_spent'].iloc[0] if len(data) > 0 else 0
            concentration = (top_customer_revenue / total_revenue * 100) if total_revenue > 0 else 0
            
            if concentration > 80:
                insights.append(f"HIGH RISK: Top customer represents {concentration:.1f}% of revenue")
                insights.append("RECOMMENDATION: Diversify customer base immediately")
            
        # Customer count analysis
        if len(data) < 5 and 'total_spent' in data.columns:
            insights.append(f"LOW CUSTOMER BASE: Only {len(data)} active customers")
            insights.append("ACTION: Focus on customer acquisition")
        
        # Product performance
        if 'revenue' in data.columns:
            top_product = data.iloc[0] if len(data) > 0 else None
            if top_product is not None:
                insights.append(f"TOP PRODUCT: {top_product.get('product_name', 'Unknown')} generating ${top_product.get('revenue', 0):,.2f}")
        
        # Average order value insights
        if 'avg_order' in data.columns:
            avg_order = data['avg_order'].mean()
            if avg_order > 500:
                insights.append(f"HIGH VALUE: Average order value of ${avg_order:.2f}")
                insights.append("OPPORTUNITY: Focus on upselling to other customers")
        
        return "\n".join(insights) if insights else "Data looks healthy! No immediate concerns detected."

@st.cache_data
def execute_ai_query(sql_query: str):
    """Execute SQL query and return results"""
    try:
        session = get_database_session()
        result = pd.read_sql(text(sql_query), session.bind)
        session.close()
        return result
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

def create_auto_chart(data: pd.DataFrame, question: str):
    """Automatically create appropriate chart based on data"""
    
    if data.empty:
        return None
    
    # Revenue by customer/product (horizontal bar chart)
    if 'total_spent' in data.columns or 'revenue' in data.columns:
        value_col = 'total_spent' if 'total_spent' in data.columns else 'revenue'
        label_col = 'name' if 'name' in data.columns else 'product_name'
        
        fig = px.bar(
            data.head(10),  # Top 10 only
            x=value_col,
            y=label_col,
            orientation='h',
            title=f"Analysis: {question.title()}",
            color=value_col,
            color_continuous_scale='Blues'
        )
        fig.update_layout(height=400)
        return fig
    
    # Order count analysis
    elif 'order_count' in data.columns:
        fig = px.scatter(
            data,
            x='order_count',
            y='total_spent',
            size='avg_order',
            hover_name='name',
            title="Customer Analysis: Orders vs Revenue",
            labels={'order_count': 'Number of Orders', 'total_spent': 'Total Revenue ($)'}
        )
        return fig
    
    # Category analysis
    elif 'category' in data.columns:
        fig = px.pie(
            data,
            values='revenue',
            names='category', 
            title="Revenue by Product Category"
        )
        return fig
    
    return None

def main():
    # Header
    st.markdown("# DataMilo - AI Analytics Assistant")
    st.markdown("### Ask questions about your data in plain English!")
    
    # Initialize AI assistant
    milo = DataMiloAI()
    
    # Sidebar for AI interaction
    st.sidebar.header("Chat with Your Data")
    
    # Pre-built questions for easy access
    st.sidebar.markdown("**Quick Questions:**")
    quick_questions = [
        "Who are my top customers?",
        "Show me revenue by product",
        "Give me customer analysis", 
        "What's my sales summary?"
    ]
    
    selected_question = st.sidebar.selectbox("Choose a question:", [""] + quick_questions)
    
    # Custom question input
    custom_question = st.sidebar.text_input("Or ask your own question:")
    
    # Use selected or custom question
    question = custom_question if custom_question else selected_question
    
    if question:
        with st.spinner(f"Analyzing: {question}"):
            # Convert to SQL
            sql_query = milo.text_to_sql(question)
            
            # Execute query
            result_data = execute_ai_query(sql_query)
            
            if not result_data.empty:
                # Display results
                st.markdown(f"## Analysis: {question}")
                
                # Show the data
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    # Auto-generate chart
                    chart = create_auto_chart(result_data, question)
                    if chart:
                        st.plotly_chart(chart, use_container_width=True)
                    
                    # Show data table
                    st.markdown("### Data Results")
                    st.dataframe(result_data, use_container_width=True)
                
                with col2:
                    # AI-generated insights
                    st.markdown("### AI Insights")
                    insights = milo.generate_insights(result_data, question)
                    st.markdown(insights)
                    
                    # Show the SQL query
                    with st.expander("View SQL Query"):
                        st.code(sql_query, language='sql')
            else:
                st.warning("No data found for your question. Try a different query!")
    
    # Add existing dashboard below
    st.markdown("---")
    st.markdown("## Standard Dashboard")
    
    # Load your existing dashboard data
    with st.spinner("Loading dashboard data..."):
        try:
            session = get_database_session()
            
            # Basic metrics
            users_df = pd.read_sql("SELECT * FROM dim_users", session.bind)
            products_df = pd.read_sql("SELECT * FROM dim_products", session.bind)
            sales_df = pd.read_sql("SELECT * FROM fact_sales", session.bind)
            
            session.close()
            
            # Display metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Customers", len(users_df))
            with col2:
                st.metric("Total Products", len(products_df))
            with col3:
                st.metric("Total Sales", len(sales_df))
            with col4:
                total_revenue = sales_df['total_amount'].sum() if len(sales_df) > 0 else 0
                st.metric("Total Revenue", f"${total_revenue:,.2f}")
                
        except Exception as e:
            st.error(f"Error loading dashboard: {e}")

if __name__ == "__main__":
    main()