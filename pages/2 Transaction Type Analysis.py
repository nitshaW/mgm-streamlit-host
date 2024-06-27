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

st.title("Transaction Type Analysis")

if df is not None:
    df['Event Date'] = pd.to_datetime(df['Event Date'], errors='coerce')
    df['Transaction Date'] = pd.to_datetime(df['Transaction Date'], errors='coerce')

    df['YearMonth'] = df['Event Date'].dt.to_period('M').astype(str)

    st.sidebar.header("Filters")

    filter_type = st.sidebar.radio("Select Date Filter Type", ("Event Date", "Transaction Date"))
    if filter_type == "Event Date":
        date_filter = st.sidebar.date_input("Select Event Date Range", [])
    else:
        date_filter = st.sidebar.date_input("Select Transaction Date Range", [])

    selected_venue_type = st.sidebar.multiselect("Select Venue Type", df["Venue Type"].unique())

    if selected_venue_type:
        df_pay_type_filtered = df[df["Venue Type"].isin(selected_venue_type)]
        selected_pay_type = st.sidebar.multiselect("Select Pay Type", df_pay_type_filtered["Pay Type"].unique())
    else:
        selected_pay_type = st.sidebar.multiselect("Select Pay Type", df["Pay Type"].unique())

    if selected_pay_type:
        df_venue_name_filtered = df_pay_type_filtered[df_pay_type_filtered["Pay Type"].isin(selected_pay_type)]
        selected_venue_name = st.sidebar.multiselect("Select Venue Name", df_venue_name_filtered["Venue Name"].unique())
    else:
        selected_venue_name = st.sidebar.multiselect("Select Venue Name", df["Venue Name"].unique())

    if selected_venue_name:
        df_item_name_filtered = df_venue_name_filtered[df_venue_name_filtered["Venue Name"].isin(selected_venue_name)]
        selected_item_name = st.sidebar.multiselect("Select Item Name", df_item_name_filtered["Item Name"].unique())
    else:
        selected_item_name = st.sidebar.multiselect("Select Item Name", df["Item Name"].unique())

    show_average = st.sidebar.checkbox("Show Average")

    if date_filter and len(date_filter) == 2:
        start_date, end_date = date_filter
        if filter_type == "Event Date":
            df = df[(df["Event Date"] >= pd.Timestamp(start_date)) & (df["Event Date"] <= pd.Timestamp(end_date))]
        else:
            df = df[(df["Transaction Date"] >= pd.Timestamp(start_date)) & (df["Transaction Date"] <= pd.Timestamp(end_date))]

    # Apply selected filters
    if selected_venue_type:
        df = df[df["Venue Type"].isin(selected_venue_type)]
    if selected_pay_type:
        df = df[df["Pay Type"].isin(selected_pay_type)]
    if selected_venue_name:
        df = df[df["Venue Name"].isin(selected_venue_name)]
    if selected_item_name:
        df = df[df["Item Name"].isin(selected_item_name)]

    # Define grouping based on selections
    group_by_cols = []
    if selected_venue_type:
        group_by_cols.append('Venue Type')
    if selected_pay_type:
        group_by_cols.append('Pay Type')
    if selected_venue_name:
        group_by_cols.append('Venue Name')
    if selected_item_name:
        group_by_cols.append('Item Name')

    # Group the filtered dataframe
    if group_by_cols:
        df_grouped = df.groupby(['YearMonth'] + group_by_cols).agg({
            'Value': ['sum', 'mean'] if show_average else ['sum'],
            'Guests': ['sum', 'mean'] if show_average else ['sum'],
            'Quantity': ['sum', 'mean'] if show_average else ['sum'],
            'Cart ID': pd.Series.nunique
        }).reset_index()

        col_names = ['YearMonth'] + group_by_cols + ['Value_sum', 'Value_avg', 'Guests_sum', 'Guests_avg', 'Quantity_sum', 'Quantity_avg', 'TRANSACTION_COUNT'] if show_average else ['YearMonth'] + group_by_cols + ['Value_sum', 'Guests_sum', 'Quantity_sum', 'TRANSACTION_COUNT']
        df_grouped.columns = col_names

        if df_grouped.empty:
            st.error("No data found to display.")
        else:
            summary_tab = st.sidebar.expander("Data Summary")
            with summary_tab:
                st.write("Summary Statistics")
                st.write(df[['Value', 'Guests', 'Quantity', 'Cart ID']].describe())

            value_chart_tab, value_dataframe_tab = st.tabs(["Chart", "Tabular Data"])

            with value_chart_tab:
                fig1 = go.Figure()
                fig2 = go.Figure()
                fig3 = go.Figure()
                fig4 = go.Figure()

                df_grouped = df_grouped.sort_values(by="YearMonth")

                for name, group in df_grouped.groupby(group_by_cols):
                    name_str = ' - '.join(map(str, name)) if isinstance(name, tuple) else name

                    fig1.add_trace(go.Scatter(x=group["YearMonth"], y=group["Value_sum"], mode='lines+markers', name=f'{name_str} Transaction Value Sum', hovertemplate='%{x}<br>Value Sum: %{y}<br>Category: ' + name_str))
                    if show_average:
                        fig1.add_trace(go.Scatter(x=group["YearMonth"], y=group["Value_avg"], mode='lines+markers', name=f'{name_str} Monthly Average Transaction Value', line=dict(dash='dot'), hovertemplate='%{x}<br>Average Value: %{y}<br>Category: ' + name_str))

                    fig2.add_trace(go.Scatter(x=group["YearMonth"], y=group["Guests_sum"], mode='lines+markers', name=f'{name_str} Guests Sum', hovertemplate='%{x}<br>Guests Sum: %{y}<br>Category: ' + name_str))
                    if show_average:
                        fig2.add_trace(go.Scatter(x=group["YearMonth"], y=group["Guests_avg"], mode='lines+markers', name=f'{name_str} Monthly Average Guests', line=dict(dash='dot'), hovertemplate='%{x}<br>Average Guests: %{y}<br>Category: ' + name_str))

                    fig3.add_trace(go.Scatter(x=group["YearMonth"], y=group["Quantity_sum"], mode='lines+markers', name=f'{name_str} Quantity Sum', hovertemplate='%{x}<br>Quantity Sum: %{y}<br>Category: ' + name_str))
                    if show_average:
                        fig3.add_trace(go.Scatter(x=group["YearMonth"], y=group["Quantity_avg"], mode='lines+markers', name=f'{name_str} Monthly Average Quantity', line=dict(dash='dot'), hovertemplate='%{x}<br>Average Quantity: %{y}<br>Category: ' + name_str))

                    fig4.add_trace(go.Scatter(x=group["YearMonth"], y=group["TRANSACTION_COUNT"], mode='lines+markers', name=f'{name_str} Transaction Counts', hovertemplate='%{x}<br>Transaction Count: %{y}<br>Category: ' + name_str))

                fig1.update_layout(height=400, title="Transaction Value Analysis", xaxis_title="Year-Month", yaxis_title="Transaction Value", xaxis=dict(type='category', categoryorder='category ascending'))
                fig2.update_layout(height=400, title="Guest Analysis", xaxis_title="Year-Month", yaxis_title="Guests", xaxis=dict(type='category', categoryorder='category ascending'))
                fig3.update_layout(height=400, title="Quantity Analysis", xaxis_title="Year-Month", yaxis_title="Quantity", xaxis=dict(type='category', categoryorder='category ascending'))
                fig4.update_layout(height=400, title="Transaction Counts", xaxis_title="Year-Month", yaxis_title="Transaction Counts", xaxis=dict(type='category', categoryorder='category ascending'))

                st.plotly_chart(fig1, use_container_width=True)
                st.plotly_chart(fig2, use_container_width=True)
                st.plotly_chart(fig3, use_container_width=True)
                st.plotly_chart(fig4, use_container_width=True)

            with value_dataframe_tab:
                st.write("Transaction Value Data")
                transaction_value_columns = ['YearMonth'] + group_by_cols + ['Value_sum']
                if show_average:
                    transaction_value_columns.append('Value_avg')
                st.dataframe(df_grouped[transaction_value_columns], height=400, width=1000)
                csv_transaction_value = convert_df_to_csv(df_grouped[transaction_value_columns])
                st.download_button(label="Download Transaction Value Data as CSV", data=csv_transaction_value, file_name='transaction_value_data.csv', mime='text/csv')

                st.write("Guest Data")
                guest_columns = ['YearMonth'] + group_by_cols + ['Guests_sum']
                if show_average:
                    guest_columns.append('Guests_avg')
                st.dataframe(df_grouped[guest_columns], height=400, width=1000)
                csv_guests = convert_df_to_csv(df_grouped[guest_columns])
                st.download_button(label="Download Guest Data as CSV", data=csv_guests, file_name='guest_data.csv', mime='text/csv')

                st.write("Quantity Data")
                quantity_columns = ['YearMonth'] + group_by_cols + ['Quantity_sum']
                if show_average:
                    quantity_columns.append('Quantity_avg')
                st.dataframe(df_grouped[quantity_columns], height=400, width=1000)
                csv_quantity = convert_df_to_csv(df_grouped[quantity_columns])
                st.download_button(label="Download Quantity Data as CSV", data=csv_quantity, file_name='quantity_data.csv', mime='text/csv')

                st.write("Transaction Counts Data")
                transaction_counts_columns = ['YearMonth'] + group_by_cols + ['TRANSACTION_COUNT']
                st.dataframe(df_grouped[transaction_counts_columns], height=400, width=1000)
                csv_transaction_counts = convert_df_to_csv(df_grouped[transaction_counts_columns])
                st.download_button(label="Download Transaction Counts Data as CSV", data=csv_transaction_counts, file_name='transaction_counts_data.csv', mime='text/csv')
    else:
        st.error("No grouping filters selected.")
else:
    st.error("Failed to retrieve data.")
