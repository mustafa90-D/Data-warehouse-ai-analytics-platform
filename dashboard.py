#!/usr/bin/env python3
"""
Simple Data Warehouse Analytics Dashboard
Works with your exact database schema
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os

# Add the project root to the path
sys.path.append(os.path.dirname(__file__))

try:
    from config.database import get_database_session
    from sqlalchemy import text
except ImportError as e:
    st.error(f"Error importing modules: {e}")
    st.stop()

# Page configuration
st.set_page_config(
    page_title="📊 Data Warehouse Analytics",
    page_icon="📊",
    layout="wide"
)

def load_simple_data():
    """Load data from your actual tables"""
    try:
        session = get_database_session()
        
        # Simple queries that work with your schema
        users_df = pd.read_sql("SELECT * FROM dim_users", session.bind)
        products_df = pd.read_sql("SELECT * FROM dim_products", session.bind)
        sales_df = pd.read_sql("SELECT * FROM fact_sales", session.bind)
        dates_df = pd.read_sql("SELECT * FROM dim_date", session.bind)
        
        session.close()
        return users_df, products_df, sales_df, dates_df
        
    except Exception as e:
        st.error(f"Database error: {e}")
        return None, None, None, None

def main():
    # Title
    st.title("📊 Data Warehouse Analytics Dashboard")
    st.markdown("### Your Business Intelligence Center")
    
    # Load data
    with st.spinner("Loading data from PostgreSQL warehouse..."):
        users_df, products_df, sales_df, dates_df = load_simple_data()
    
    if users_df is None:
        st.error("❌ Could not load data. Make sure PostgreSQL is running and data is loaded.")
        st.info("💡 Try running: `python main.py` to load sample data first.")
        return
    
    # Success message
    st.success(f"✅ Connected to data warehouse! Found {len(sales_df)} sales transactions.")
    
    # Key Metrics
    st.markdown("---")
    st.subheader("📈 Key Business Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("👥 Total Customers", len(users_df))
    
    with col2:
        st.metric("📦 Total Products", len(products_df))
    
    with col3:
        st.metric("💰 Total Sales", len(sales_df))
    
    with col4:
        total_revenue = sales_df['total_amount'].sum() if len(sales_df) > 0 else 0
        st.metric("💵 Total Revenue", f"${total_revenue:,.2f}")
    
    # Charts Section
    st.markdown("---")
    st.subheader("📊 Analytics Charts")
    
    if len(sales_df) > 0:
        col1, col2 = st.columns(2)
        
        with col1:
            # Sales by Product
            if 'product_id' in sales_df.columns:
                product_sales = sales_df.groupby('product_id')['total_amount'].sum().reset_index()
                product_sales = product_sales.merge(products_df[['product_id', 'product_name']], on='product_id', how='left')
                
                fig1 = px.bar(
                    product_sales, 
                    x='product_name', 
                    y='total_amount',
                    title="💰 Revenue by Product",
                    labels={'total_amount': 'Revenue ($)', 'product_name': 'Product'}
                )
                fig1.update_layout(xaxis_tickangle=45)
                st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            # Sales by Customer
            if 'user_id' in sales_df.columns:
                customer_sales = sales_df.groupby('user_id')['total_amount'].sum().reset_index()
                customer_sales = customer_sales.merge(users_df[['user_id', 'name']], on='user_id', how='left')
                
                fig2 = px.bar(
                    customer_sales.sort_values('total_amount', ascending=False),
                    x='name', 
                    y='total_amount',
                    title="👥 Revenue by Customer",
                    labels={'total_amount': 'Revenue ($)', 'name': 'Customer'}
                )
                fig2.update_layout(xaxis_tickangle=45)
                st.plotly_chart(fig2, use_container_width=True)
    
    # Data Tables
    st.markdown("---")
    st.subheader("📋 Data Tables")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Sales", "Customers", "Products", "Dates"])
    
    with tab1:
        st.write("**Sales Transactions**")
        if len(sales_df) > 0:
            st.dataframe(sales_df, use_container_width=True)
        else:
            st.info("No sales data found.")
    
    with tab2:
        st.write("**Customer Directory**")
        st.dataframe(users_df, use_container_width=True)
    
    with tab3:
        st.write("**Product Catalog**")
        st.dataframe(products_df, use_container_width=True)
    
    with tab4:
        st.write("**Date Dimension**")
        st.dataframe(dates_df, use_container_width=True)
    
    # Business Insights
    if len(sales_df) > 0:
        st.markdown("---")
        st.subheader("💡 Business Insights")
        
        # Top customer
        if 'user_id' in sales_df.columns and len(users_df) > 0:
            customer_totals = sales_df.groupby('user_id')['total_amount'].sum()
            top_customer_id = customer_totals.idxmax()
            top_customer_revenue = customer_totals.max()
            top_customer_name = users_df[users_df['user_id'] == top_customer_id]['name'].iloc[0]
            
            st.success(f"🏆 **Top Customer**: {top_customer_name} with ${top_customer_revenue:,.2f} in total purchases")
        
        # Average order value
        avg_order = sales_df['total_amount'].mean()
        st.info(f"📊 **Average Order Value**: ${avg_order:.2f}")
        
        # Product performance
        if len(products_df) > 0:
            st.info(f"📦 **Product Catalog**: {len(products_df)} products available")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        🏗️ Built with Streamlit | 🗄️ PostgreSQL Data Warehouse | ⚡ Real-time Analytics
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()