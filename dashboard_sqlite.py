#!/usr/bin/env python3
"""
Custom AI Analytics Dashboard - Your Branded Version
Professional AI-powered data warehouse analytics
"""
import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, date

# Configure page
st.set_page_config(
    page_title="AI Analytics Assistant",
    page_icon="📊",
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
        # Users (same as your PostgreSQL data)
        users_data = [
            (1, 'Leanne Graham', 'Sincere@april.biz', 'Gwenborough', '1-770-736-8031 x56442', 'Romaguera-Crona', datetime.now()),
            (2, 'Ervin Howell', 'Shanna@melissa.tv', 'Wisokyburgh', '010-692-6593 x09125', 'Deckow-Crist', datetime.now()),
            (3, 'Clementine Bauch', 'Nathan@yesenia.net', 'McKenziehaven', '1-463-123-4447', 'Romaguera-Jacobson', datetime.now()),
            (4, 'Patricia Lebsack', 'Julianne.OConner@kory.org', 'South Elvis', '493-170-9623 x156', 'Robel-Corkery', datetime.now()),
            (5, 'Chelsey Dietrich', 'Lucio_Hettinger@annie.ca', 'Roscoeview', '(254)954-1289', 'Keebler LLC', datetime.now())
        ]
        
        cursor.executemany('INSERT INTO dim_users VALUES (?, ?, ?, ?, ?, ?, ?)', users_data)
        
        # Products
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
        
        # Dates
        date_data = [
            (20240101, date(2024, 1, 1), 2024, 1, 1, 1, 'Monday', 'January', 'No'),
            (20240115, date(2024, 1, 15), 2024, 1, 15, 1, 'Monday', 'January', 'No'),
            (20240201, date(2024, 2, 1), 2024, 2, 1, 1, 'Thursday', 'February', 'No'),
            (20240315, date(2024, 3, 15), 2024, 3, 15, 1, 'Friday', 'March', 'No'),
            (20240420, date(2024, 4, 20), 2024, 4, 20, 2, 'Saturday', 'April', 'Yes')
        ]
        
        cursor.executemany('INSERT INTO dim_date VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', date_data)
        
        # Sales (matching your business scenario with Ervin dominating)
        sales_data = [
            (1, 2, 1, 20240101, 1299.99, 2, 2599.98, datetime.now()),  # Ervin: 2 Laptops
            (2, 2, 7, 20240115, 79.99, 1, 79.99, datetime.now()),     # Ervin: 1 Keyboard  
            (3, 2, 6, 20240201, 45.99, 5, 229.95, datetime.now()),    # Ervin: 5 Desk Lamps
            (4, 2, 2, 20240315, 29.99, 3, 89.97, datetime.now()),     # Ervin: 3 Mice
            (5, 5, 4, 20240420, 12.99, 1, 12.99, datetime.now()),     # Chelsey: 1 Mug
            (6, 1, 8, 20240420, 19.99, 1, 19.99, datetime.now())      # Leanne: 1 Water Bottle
        ]
        
        cursor.executemany('INSERT INTO fact_sales VALUES (?, ?, ?, ?, ?, ?, ?, ?)', sales_data)
        
        conn.commit()
        return conn
        
    except Exception as e:
        st.error(f"Error creating sample database: {e}")
        return None

class AnalyticsAI:
    """AI-powered analytics assistant"""
    
    def text_to_sql(self, question: str) -> str:
        """Convert natural language question to SQL query"""
        
        # Predefined SQL queries for common questions
        query_mapping = {
            "top customers": """
                SELECT u.name, SUM(s.total_amount) as total_spent
                FROM fact_sales s
                JOIN dim_users u ON s.user_id = u.user_id
                GROUP BY u.user_id, u.name
                ORDER BY total_spent DESC
                LIMIT 10
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
            """,
            "product performance": """
                SELECT 
                    p.product_name,
                    p.category,
                    p.price,
                    COUNT(s.sale_id) as sales_count,
                    SUM(s.quantity) as total_quantity_sold,
                    SUM(s.total_amount) as total_revenue
                FROM fact_sales s
                JOIN dim_products p ON s.product_id = p.product_id
                GROUP BY p.product_id, p.product_name, p.category, p.price
                ORDER BY total_revenue DESC
            """
        }
        
        # Simple keyword matching
        question_lower = question.lower()
        
        if "top customer" in question_lower or "best customer" in question_lower:
            return query_mapping["top customers"]
        elif "revenue by product" in question_lower or "product performance" in question_lower:
            return query_mapping["product performance"]
        elif "customer analysis" in question_lower or "customer breakdown" in question_lower:
            return query_mapping["customer analysis"]
        elif "sales summary" in question_lower or "overview" in question_lower:
            return query_mapping["sales summary"]
        else:
            return query_mapping["sales summary"]
    
    def generate_insights(self, data: pd.DataFrame, question: str) -> list:
        """Generate business insights from query results"""
        
        if data.empty:
            return ["❌ No data found for your query."]
        
        insights = []
        
        # Revenue concentration analysis
        if 'total_spent' in data.columns and len(data) > 0:
            total_revenue = data['total_spent'].sum()
            top_customer_revenue = data['total_spent'].iloc[0] if len(data) > 0 else 0
            concentration = (top_customer_revenue / total_revenue * 100) if total_revenue > 0 else 0
            
            insights.append("**💰 Revenue Analysis:**")
            insights.append(f"• Top customer: **{data.iloc[0]['name']}** (${top_customer_revenue:,.2f})")
            insights.append(f"• Market share: **{concentration:.1f}%** of total revenue")
            
            if concentration > 80:
                insights.append("• 🚨 **CRITICAL RISK**: Extreme customer concentration!")
                insights.append("• 📈 **RECOMMENDATION**: Diversify customer base immediately")
                insights.append("• 💡 **IMPACT**: Losing this customer would devastate revenue")
            elif concentration > 50:
                insights.append("• ⚠️ **MODERATE RISK**: High customer concentration")
                insights.append("• 🎯 **SUGGESTION**: Develop customer acquisition strategy")
        
        # Customer portfolio analysis
        if 'total_spent' in data.columns:
            customer_count = len(data)
            avg_customer_value = data['total_spent'].mean() if customer_count > 0 else 0
            
            insights.append("")
            insights.append("**👥 Customer Portfolio:**")
            insights.append(f"• Active customers: **{customer_count}**")
            insights.append(f"• Average customer value: **${avg_customer_value:,.2f}**")
            
            if customer_count < 10:
                insights.append("• 🎯 **PRIORITY**: Expand customer base - high growth potential")
            
            # VIP customer identification
            if len(data) > 0:
                vip_threshold = data['total_spent'].quantile(0.8)
                vip_customers = data[data['total_spent'] >= vip_threshold]
                insights.append(f"• 👑 **VIP Customers**: {len(vip_customers)} driving majority of revenue")
        
        # Product performance insights
        if 'total_revenue' in data.columns and 'product_name' in data.columns and len(data) > 0:
            top_product = data.iloc[0]
            total_product_revenue = data['total_revenue'].sum()
            top_product_share = (top_product['total_revenue'] / total_product_revenue * 100) if total_product_revenue > 0 else 0
            
            insights.append("")
            insights.append("**📦 Product Performance:**")
            insights.append(f"• Best seller: **{top_product['product_name']}**")
            insights.append(f"• Revenue: **${top_product['total_revenue']:,.2f}** ({top_product_share:.1f}% of total)")
            insights.append(f"• Category: **{top_product.get('category', 'Unknown')}**")
            
            if 'sales_count' in data.columns:
                insights.append(f"• Sales volume: **{top_product['sales_count']} transactions**")
                
            # Category analysis
            if len(data) > 1:
                category_performance = data.groupby('category')['total_revenue'].sum().sort_values(ascending=False)
                top_category = category_performance.index[0]
                insights.append(f"• Leading category: **{top_category}** (${category_performance.iloc[0]:,.2f})")
        
        # Business summary insights
        if 'total_revenue' in data.columns and len(data) == 1:  # Sales summary query
            row = data.iloc[0]
            insights.append("**📊 Business Summary:**")
            insights.append(f"• Total transactions: **{row.get('total_sales', 0):,}**")
            insights.append(f"• Total revenue: **${row.get('total_revenue', 0):,.2f}**")
            insights.append(f"• Average order value: **${row.get('avg_order_value', 0):,.2f}**")
            insights.append(f"• Unique customers: **{row.get('unique_customers', 0)}**")
            
            # Business health indicators
            aov = row.get('avg_order_value', 0)
            if aov > 1000:
                insights.append("• 💎 **PREMIUM BUSINESS**: Excellent average order value!")
                insights.append("• 🎯 **STRATEGY**: Focus on customer retention and premium service")
            elif aov > 500:
                insights.append("• 💰 **STRONG PERFORMANCE**: Solid average order value")
                insights.append("• 📈 **OPPORTUNITY**: Upselling could drive significant growth")
            elif aov > 100:
                insights.append("• 📈 **GROWTH POTENTIAL**: Opportunity to increase order values")
                insights.append("• 💡 **TACTICS**: Consider product bundling or cross-selling")
        
        return insights if insights else ["✅ Data analysis complete. No specific insights detected."]

def execute_query(sql_query: str, conn):
    """Execute SQL query and return results"""
    try:
        result = pd.read_sql_query(sql_query, conn)
        return result
    except Exception as e:
        st.error(f"❌ Query execution error: {e}")
        return pd.DataFrame()

def main():
    # Header with professional styling
    st.markdown("""
    <div style="background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%); padding: 2.5rem; border-radius: 15px; margin-bottom: 2rem; box-shadow: 0 10px 30px rgba(0,0,0,0.1);">
        <h1 style="color: white; margin: 0; text-align: center; font-size: 2.5em; font-weight: 700;">📊 AI Analytics Assistant</h1>
        <p style="color: #e8f0fe; margin: 1rem 0 0 0; text-align: center; font-size: 1.3em;">Intelligent Data Warehouse Analytics Platform</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Create sample database
    conn = create_sample_database()
    if conn is None:
        st.error("❌ Could not create data warehouse.")
        st.stop()
    else:
        st.success("✅ Data warehouse connected and ready!")
        st.info("🎯 Professional star schema with dimensional modeling and fact tables")
    
    # Initialize AI assistant
    ai_assistant = AnalyticsAI()
    
    # Main interface
    st.markdown("### 🎯 Natural Language Query Interface")
    
    # Create columns for layout
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Question input
        question_options = [
            "Select a question...",
            "Who are my top customers?",
            "Show me product performance", 
            "Give me customer analysis",
            "What's my sales summary?",
            "Show me revenue by product"
        ]
        
        selected_question = st.selectbox("**Quick Analytics Questions:**", question_options)
        custom_question = st.text_input("**Custom Analytics Query:**", placeholder="e.g., Which customers drive the most revenue?")
        
    with col2:
        st.markdown("") # spacing
        st.markdown("") # spacing
        analyze_button = st.button("🔍 **Generate Insights**", type="primary", use_container_width=True)
    
    # Process question
    if analyze_button:
        question = custom_question if custom_question.strip() else selected_question
        
        if question and question != "Select a question...":
            # Show analysis
            st.markdown("---")
            st.markdown(f"## 📊 Analytics Results: *{question}*")
            
            with st.spinner(f"🤖 Processing analytics query: {question}"):
                # Convert to SQL
                sql_query = ai_assistant.text_to_sql(question)
                
                # Execute query
                result_data = execute_query(sql_query, conn)
                
                if not result_data.empty:
                    # Create layout for results
                    data_col, insights_col = st.columns([3, 2])
                    
                    with data_col:
                        st.markdown("### 📋 Query Results")
                        
                        # Format numbers nicely for display
                        display_data = result_data.copy()
                        for col in display_data.columns:
                            if 'spent' in col or 'revenue' in col or 'amount' in col or 'price' in col:
                                if display_data[col].dtype in ['float64', 'int64']:
                                    display_data[col] = display_data[col].apply(lambda x: f"${x:,.2f}" if pd.notnull(x) else "")
                        
                        st.dataframe(display_data, use_container_width=True, hide_index=True)
                    
                    with insights_col:
                        st.markdown("### 🎯 Business Insights")
                        insights = ai_assistant.generate_insights(result_data, question)
                        
                        # Display insights in nice format
                        for insight in insights:
                            st.markdown(insight)
                    
                    # Show SQL query in expandable section
                    with st.expander("🔧 View Generated SQL Query"):
                        st.code(sql_query, language='sql')
                        
                    # Add export option
                    csv = result_data.to_csv(index=False)
                    st.download_button(
                        label="📥 Export Results to CSV",
                        data=csv,
                        file_name=f"analytics_results_{question.lower().replace(' ', '_')}.csv",
                        mime="text/csv"
                    )
                        
                else:
                    st.warning("⚠️ No data found for your query. Try a different analytics question!")
        else:
            st.info("👆 Please select a question or enter your own analytics query!")
    
    # Dashboard overview section
    st.markdown("---")
    st.markdown("## 📈 Business Intelligence Dashboard")
    
    try:
        # Get basic metrics
        users_data = execute_query("SELECT COUNT(*) as count FROM dim_users", conn)
        products_data = execute_query("SELECT COUNT(*) as count FROM dim_products", conn)
        sales_data = execute_query("SELECT COUNT(*) as count FROM fact_sales", conn)
        revenue_data = execute_query("SELECT SUM(total_amount) as total FROM fact_sales", conn)
        
        # Display metrics in cards
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            users = users_data.iloc[0]['count'] if not users_data.empty else 0
            st.metric("👥 Customers", f"{users:,}")
            
        with col2:
            products = products_data.iloc[0]['count'] if not products_data.empty else 0
            st.metric("📦 Products", f"{products:,}")
            
        with col3:
            sales = sales_data.iloc[0]['count'] if not sales_data.empty else 0
            st.metric("💰 Sales", f"{sales:,}")
            
        with col4:
            revenue = revenue_data.iloc[0]['total'] if not revenue_data.empty else 0
            revenue = revenue if revenue is not None else 0
            st.metric("💵 Revenue", f"${revenue:,.2f}")
        
        # Recent activity
        st.markdown("### 🕒 Sales Transaction History")
        recent_sales = execute_query("""
            SELECT 
                s.sale_id,
                u.name as customer,
                p.product_name,
                s.quantity,
                s.total_amount as amount,
                date(s.created_at) as sale_date
            FROM fact_sales s
            JOIN dim_users u ON s.user_id = u.user_id
            JOIN dim_products p ON s.product_id = p.product_id
            ORDER BY s.created_at DESC
        """, conn)
        
        if not recent_sales.empty:
            # Format the amounts
            display_sales = recent_sales.copy()
            display_sales['amount'] = display_sales['amount'].apply(lambda x: f"${x:,.2f}")
            st.dataframe(display_sales, use_container_width=True, hide_index=True)
        else:
            st.info("No sales transaction data available.")
            
    except Exception as e:
        st.error(f"❌ Error loading dashboard: {e}")
    
    # Technical note
    st.markdown("---")
    st.markdown("""
    <div style="background: #f8f9fa; padding: 1.5rem; border-radius: 8px; margin: 1rem 0; border-left: 4px solid #2a5298;">
        <h4>🏗️ Technical Architecture</h4>
        <p><strong>This demonstrates a complete data warehousing solution:</strong></p>
        <ul>
            <li>✅ <strong>Star Schema Design</strong> - Dimensional modeling with fact and dimension tables</li>
            <li>✅ <strong>Natural Language Processing</strong> - AI converts questions to SQL queries</li>
            <li>✅ <strong>Business Intelligence</strong> - Automated insights and risk analysis</li>
            <li>✅ <strong>Real-time Analytics</strong> - Instant query processing and visualization</li>
            <li>✅ <strong>Data Governance</strong> - Structured ETL pipeline and data validation</li>
        </ul>
        <p><em>Enterprise-grade architecture ready for production deployment with PostgreSQL, Docker, and cloud infrastructure.</em></p>
    </div>
    """, unsafe_allow_html=True)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #666; padding: 1rem;">
        <p>🚀 <strong>AI Analytics Assistant</strong> - Enterprise Data Warehouse Platform</p>
        <p>Powered by dimensional modeling, natural language processing, and automated business intelligence</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()