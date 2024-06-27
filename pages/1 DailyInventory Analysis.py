import streamlit as st
from snowflake.snowpark import Session
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout="wide")
st.title("Daily Inventory Analysis")

sel = "Inventory VS Quantity Counts"
st.markdown(f"**{sel}**")
value_chart_tab, value_dataframe_tab = st.tabs(["Chart", "Tabular Data"])
summary_tab = st.sidebar.expander("Data Summary")

# Define a function to get a Snowflake session
@st.cache_resource
def get_session():
    try:
        return get_active_session()
    except:
        pars = {
            "account": st.secrets["snowflake"]["account"],
            "user": st.secrets["snowflake"]["user"],
            "password": st.secrets["snowflake"]["password"],
            "warehouse": st.secrets["snowflake"]["warehouse"],
            "role": st.secrets["snowflake"]["role"],
            "database": st.secrets["snowflake"]["database"]
        }
        return Session.builder.configs(pars).create()

# Define a function to execute a query and return a DataFrame
@st.cache_data
def get_dataframe(query):
    session = get_session()
    if session is None:
        st.error("Session is not initialized.")
        return None
    try:
        # Execute query and fetch results
        snow_df = session.sql(query).to_pandas()

        # Perform preprocessing on Snowflake using Snowpark DataFrame operations
        snow_df = snow_df.drop_duplicates()
        snow_df = snow_df.dropna(subset=['TI_CALDATE', 'TB_TRANSDATE'])  # Drop rows with null dates

        # Replace null EF_NAME and TI_ITEMNAME with 'Unknown'
        snow_df['EF_NAME'] = snow_df['EF_NAME'].fillna('Unknown')
        snow_df['TI_ITEMNAME'] = snow_df['TI_ITEMNAME'].fillna('Unknown')
        snow_df['TB_GLOBALTYPE'] = snow_df['TB_GLOBALTYPE'].fillna('Unknown')
        snow_df['VT_NAME'] = snow_df['VT_NAME'].fillna('Unknown')
        snow_df['VP_VENUENAME'] = snow_df['VP_VENUENAME'].fillna('Unknown')

        # Rename columns
        snow_df.rename(columns={
            'VP_VENUENAME': 'Venue Name',
            'VT_NAME': 'Venue Type',
            'TB_GLOBALTYPE': 'Pay Type',
            'EF_NAME': 'Event Name',
            'TI_ITEMNAME': 'Item Name',
            'TB_QTY': 'Quantity',
            'STOCK': 'Stock',
            'TB_SUBTOTALAGREE': 'Value',
            'TB_GUESTS': 'Guests',
            'TB_CARTID': 'Cart ID',
            'TI_CALDATE': 'Event Date',
            'TB_TRANSDATE': 'Transaction Date'
        }, inplace=True)

        return snow_df
    except Exception as e:
        st.error(f"Failed to execute query or process data: {str(e)}")
        return None

# Function to convert DataFrame to CSV
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

# SQL query with fully qualified table name
query = """
    SELECT 
        *
    FROM 
        SALES_ANALYTICS.PUBLIC.MGM_TRANSACTIONS
    WHERE 
        TB_ACTION = 'charge'
"""

# Use the function to retrieve data
df = get_dataframe(query)

# Check if df is not None before applying filters
if df is not None:
    # Ensure Event Date and Transaction Date are in datetime format
    df['Event Date'] = pd.to_datetime(df['Event Date'], errors='coerce')
    df['Transaction Date'] = pd.to_datetime(df['Transaction Date'], errors='coerce')

    # Drop rows with invalid dates
    df = df.dropna(subset=['Event Date', 'Transaction Date'])

    # Interactive filters
    st.sidebar.header("Filters")

    caldate_filter = st.sidebar.date_input("Select Event Date Range", [])
    if caldate_filter and len(caldate_filter) == 2:
        start_date, end_date = caldate_filter
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        df = df[(df["Event Date"] >= start_date) & (df["Event Date"] <= end_date)]
    else:
        # Default to previous month if no date range is selected
        end_date = pd.Timestamp.now()
        start_date = end_date - pd.DateOffset(months=1)
        df = df[(df["Event Date"] >= start_date) & (df["Event Date"] <= end_date)]

    # Filter based on Venue Name
    vp_venuename_filter = st.sidebar.selectbox("Select Venue Name", df["Venue Name"].unique())
    df = df[df["Venue Name"] == vp_venuename_filter]

    # Populate Event Name dropdown based on Venue Name and Event Date range
    ef_name_options = df["Event Name"].unique()
    ef_name_filter = st.sidebar.selectbox("Select Event Name", ["All"] + list(ef_name_options))
    if ef_name_filter != "All":
        df = df[df["Event Name"] == ef_name_filter]

    # Filter based on Item Name
    ti_itemname_options = df["Item Name"].unique()
    ti_itemname_filter = st.sidebar.selectbox("Select Item Name", ["All"] + list(ti_itemname_options))
    if ti_itemname_filter != "All":
        df = df[df["Item Name"] == ti_itemname_filter]

    # Sort by Event Date
    df = df.sort_values(by='Event Date')
    df['Date'] = df['Event Date']
    date_label = "Event Date"

    # Determine grouping based on filters
    if ef_name_filter != "All":
        group_by_cols = ['Date', 'Venue Name', 'Event Name', 'Item Name']
    else:
        group_by_cols = ['Date', 'Venue Name', 'Item Name']

    # Group by determined columns for Quantity (sum), Value (sum), Guests (sum), and take max of Stock
    df_grouped = df.groupby(group_by_cols).agg({
        'Quantity': 'sum', 
        'Stock': 'max', 
        'Value': 'sum', 
        'Guests': 'sum',
        'Cart ID': pd.Series.nunique  # Count unique Cart IDs for transaction count
    }).reset_index()

    df_grouped.rename(columns={'Cart ID': 'TRANSACTION_COUNT'}, inplace=True)
    
    # Calculate Percentage Sale of Stock
    df_grouped['PERCENTAGE_SALE'] = (df_grouped['Quantity'] / df_grouped['Stock']) * 100
    df_grouped['PERCENTAGE_SALE'] = df_grouped['PERCENTAGE_SALE'].round(2)

    # Check if df_grouped is not None and not empty before plotting
    if df_grouped.empty:
        st.error("No data found to display.")
    else:
        # Display summary statistics
        with summary_tab:
            st.write("Summary Statistics")
            st.write(df[['Quantity', 'Stock', 'Value', 'Guests']].describe())

        # Define consistent colors for each grouping
        if ef_name_filter != "All":
            color_map = {f"{ef_name}-{ti_itemname}": color for (ef_name, ti_itemname), color in zip(
                [(ef, ti) for ef in [ef_name_filter] for ti in ti_itemname_filter],
                px.colors.qualitative.Plotly * 10)}  # Adjust the multiplication factor based on the number of combinations
        else:
            color_map = {ti_itemname: color for ti_itemname, color in zip(
                ti_itemname_filter, px.colors.qualitative.Plotly * 10)}

        # Visualization using plotly.graph_objects
        with value_chart_tab:
            fig1 = go.Figure()
            fig2 = go.Figure()
            fig3 = go.Figure()
            fig4 = go.Figure()
            fig5 = go.Figure()

            if ef_name_filter != "All":
                for itemname in ti_itemname_filter:
                    filtered_df = df_grouped[(df_grouped['Event Name'] == ef_name_filter) & (df_grouped['Item Name'] == itemname)]
                    base_color = color_map[f"{ef_name_filter}-{itemname}"]
                    fig1.add_trace(go.Scatter(x=filtered_df["Date"], y=filtered_df["Quantity"], mode='lines+markers', name=f'{ef_name_filter} - {itemname} Quantity', line=dict(color=base_color, dash='solid')))
                    fig1.add_trace(go.Scatter(x=filtered_df["Date"], y=filtered_df["Stock"], mode='lines+markers', name=f'{ef_name_filter} - {itemname} Stock', line=dict(color=base_color, dash='dot')))
                    fig2.add_trace(go.Scatter(x=filtered_df["Date"], y=filtered_df["Value"], mode='lines+markers', name=f'{ef_name_filter} - {itemname} Value', line=dict(color=base_color, dash='solid')))
                    fig3.add_trace(go.Scatter(x=filtered_df["Date"], y=filtered_df["Guests"], mode='lines+markers', name=f'{ef_name_filter} - {itemname} Guests', line=dict(color=base_color, dash='solid')))
                    fig4.add_trace(go.Scatter(x=filtered_df["Date"], y=filtered_df["TRANSACTION_COUNT"], mode='lines+markers', name=f'{ef_name_filter} - {itemname} Transactions', line=dict(color=base_color, dash='solid')))
                    fig5.add_trace(go.Scatter(x=filtered_df["Date"], y=filtered_df["PERCENTAGE_SALE"], mode='lines+markers', name=f'{ef_name_filter} - {itemname} Percentage Sale', line=dict(color=base_color, dash='solid'), hovertemplate='%{y:.2f}%<extra></extra>'))
            else:
                for itemname in ti_itemname_filter:
                    filtered_df = df_grouped[df_grouped['Item Name'] == itemname]
                    base_color = color_map[itemname]
                    fig1.add_trace(go.Scatter(x=filtered_df["Date"], y=filtered_df["Quantity"], mode='lines+markers', name=f'{itemname} Quantity', line=dict(color=base_color, dash='solid')))
                    fig1.add_trace(go.Scatter(x=filtered_df["Date"], y=filtered_df["Stock"], mode='lines+markers', name=f'{itemname} Stock', line=dict(color=base_color, dash='dot')))
                    fig2.add_trace(go.Scatter(x=filtered_df["Date"], y=filtered_df["Value"], mode='lines+markers', name=f'{itemname} Value', line=dict(color=base_color, dash='solid')))
                    fig3.add_trace(go.Scatter(x=filtered_df["Date"], y=filtered_df["Guests"], mode='lines+markers', name=f'{itemname} Guests', line=dict(color=base_color, dash='solid')))
                    fig4.add_trace(go.Scatter(x=filtered_df["Date"], y=filtered_df["TRANSACTION_COUNT"], mode='lines+markers', name=f'{itemname} Transactions', line=dict(color=base_color, dash='solid')))
                    fig5.add_trace(go.Scatter(x=filtered_df["Date"], y=filtered_df["PERCENTAGE_SALE"], mode='lines+markers', name=f'{itemname} Percentage Sale', line=dict(color=base_color, dash='solid'), hovertemplate='%{y:.2f}%<extra></extra>'))

            fig1.update_layout(height=400, title="Stock VS Quantity Counts", xaxis_title="Date", yaxis_title="Count")
            fig2.update_layout(height=400, title="Transaction Value Analysis", xaxis_title="Date", yaxis_title="Value")
            fig3.update_layout(height=400, title="Guest Analysis", xaxis_title="Date", yaxis_title="Guests")
            fig4.update_layout(height=400, title="Transaction Counts", xaxis_title="Date", yaxis_title="Transaction Count")
            fig5.update_layout(height=400, title="Percentage Sale of Stock", xaxis_title="Date", yaxis_title="Percentage")

            st.plotly_chart(fig1, use_container_width=True)
            st.plotly_chart(fig5, use_container_width=True)  # Added the new chart for Percentage Sale of Stock after Stock VS Quantity Counts
            st.plotly_chart(fig4, use_container_width=True)  # Added the new chart for Transaction Counts
            st.plotly_chart(fig2, use_container_width=True)
            st.plotly_chart(fig3, use_container_width=True)

        # Display grouped DataFrame in another container
        with value_dataframe_tab:
            st.write(f"Data from {df['Date'].min().date()} to {df['Date'].max().date()} ({date_label})")
            st.write("Quantity and Stock Data")
            st.dataframe(df_grouped[group_by_cols + ['Quantity', 'Stock']], height=300, width=1000)
            csv_quantity_stock = convert_df_to_csv(df_grouped[group_by_cols + ['Quantity', 'Stock']])
            st.download_button(label="Download Quantity and Stock Data as CSV", data=csv_quantity_stock, file_name='quantity_stock_data.csv', mime='text/csv')

            st.write("Percentage Sale of Stock Data")
            st.dataframe(df_grouped[group_by_cols + ['PERCENTAGE_SALE']], height=300, width=1000)
            csv_percentage_sale = convert_df_to_csv(df_grouped[group_by_cols + ['PERCENTAGE_SALE']])
            st.download_button(label="Download Percentage Sale of Stock Data as CSV", data=csv_percentage_sale, file_name='percentage_sale_data.csv', mime='text/csv')

            st.write("Transaction Counts Data")
            st.dataframe(df_grouped[group_by_cols + ['TRANSACTION_COUNT']], height=300, width=1000)
            csv_transaction_counts = convert_df_to_csv(df_grouped[group_by_cols + ['TRANSACTION_COUNT']])
            st.download_button(label="Download Transaction Counts Data as CSV", data=csv_transaction_counts, file_name='transaction_counts_data.csv', mime='text/csv')

            st.write("Transation Value Data")
            st.dataframe(df_grouped[group_by_cols + ['Value']], height=300, width=1000)
            csv_value = convert_df_to_csv(df_grouped[group_by_cols + ['Value']])
            st.download_button(label="Download Transaction Data as CSV", data=csv_value, file_name='value_data.csv', mime='text/csv')

            st.write("Guest Data")
            st.dataframe(df_grouped[group_by_cols + ['Guests']], height=300, width=1000)
            csv_guests = convert_df_to_csv(df_grouped[group_by_cols + ['Guests']])
            st.download_button(label="Download Guest Data as CSV", data=csv_guests, file_name='guest_data.csv', mime='text/csv')
else:
    st.error("Failed to retrieve data.")
