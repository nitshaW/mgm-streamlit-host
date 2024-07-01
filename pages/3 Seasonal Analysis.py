import streamlit as st
from snowflake.snowpark import Session
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout="wide")
st.title("Seasonal Analysis Over Time")

sel = "Market Value Analysis by Season Over Time"
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

    # Function to determine the season for a given date
    def get_season(date):
        if date.month in [12, 1, 2]:
            return 'Winter'
        elif date.month in [3, 4, 5]:
            return 'Spring'
        elif date.month in [6, 7, 8]:
            return 'Summer'
        elif date.month in [9, 10, 11]:
            return 'Fall'

    # Add season and year column
    df['Season'] = df['Event Date'].apply(get_season)
    df['Year'] = df['Event Date'].dt.year.fillna(0).astype(int)

    # Combine Year and Season for grouping
    df['YearSeason'] = df['Year'].astype(str) + ' ' + df['Season']

    # Interactive filters
    st.sidebar.header("Filters")

    filter_type = st.sidebar.radio("Select Date Filter Type", ("Event Date", "Transaction Date"))
    if filter_type == "Event Date":
        date_filter = st.sidebar.date_input("Select Event Date Range", [])
    else:
        date_filter = st.sidebar.date_input("Select Transaction Date Range", [])

    filter_option = st.sidebar.radio("Select Filter", ["None", "Venue Name", "Venue Type", "Global Type"])
    if filter_option == "Venue Name":
        filter_values = st.sidebar.multiselect("Select Venue Name", df["Venue Name"].unique())
        df_filtered = df[df["Venue Name"].isin(filter_values)]
    elif filter_option == "Venue Type":
        filter_values = st.sidebar.multiselect("Select Venue Type", df["Venue Type"].unique())
        df_filtered = df[df["Venue Type"].isin(filter_values)]
    elif filter_option == "Global Type":
        filter_values = st.sidebar.multiselect("Select Global Type", df["Global Type"].unique())
        df_filtered = df[df["Global Type"].isin(filter_values)]
    else:
        df_filtered = df
        filter_values = []

    show_average = st.sidebar.checkbox("Show Average")

    # Apply date filters
    if date_filter and len(date_filter) == 2:
        start_date, end_date = date_filter
        if filter_type == "Event Date":
            df_filtered = df_filtered[(df_filtered["Event Date"] >= pd.Timestamp(start_date)) & (df_filtered["Event Date"] <= pd.Timestamp(end_date))]
        else:
            df_filtered = df_filtered[(df_filtered["Transaction Date"] >= pd.Timestamp(start_date)) & (df_filtered["Transaction Date"] <= pd.Timestamp(end_date))]

    # Group by YearSeason and the selected filter, then aggregate Value, Guests, and Quantity
    if filter_option == "Venue Name":
        df_grouped = df_filtered.groupby(['YearSeason', 'Venue Name']).agg({
            'Value': ['sum', 'mean'] if show_average else ['sum'],
            'Guests': ['sum', 'mean'] if show_average else ['sum'],
            'Quantity': ['sum', 'mean'] if show_average else ['sum']
        }).reset_index()
        df_grouped.columns = ['YearSeason', 'Venue Name', 'Value_sum', 'Value_avg', 'Guests_sum', 'Guests_avg', 'Quantity_sum', 'Quantity_avg'] if show_average else ['YearSeason', 'Venue Name', 'Value_sum', 'Guests_sum', 'Quantity_sum']
    elif filter_option == "Venue Type":
        df_grouped = df_filtered.groupby(['YearSeason', 'Venue Type']).agg({
            'Value': ['sum', 'mean'] if show_average else ['sum'],
            'Guests': ['sum', 'mean'] if show_average else ['sum'],
            'Quantity': ['sum', 'mean'] if show_average else ['sum']
        }).reset_index()
        df_grouped.columns = ['YearSeason', 'Venue Type', 'Value_sum', 'Value_avg', 'Guests_sum', 'Guests_avg', 'Quantity_sum', 'Quantity_avg'] if show_average else ['YearSeason', 'Venue Type', 'Value_sum', 'Guests_sum', 'Quantity_sum']
    elif filter_option == "Global Type":
        df_grouped = df_filtered.groupby(['YearSeason', 'Global Type']).agg({
            'Value': ['sum', 'mean'] if show_average else ['sum'],
            'Guests': ['sum', 'mean'] if show_average else ['sum'],
            'Quantity': ['sum', 'mean'] if show_average else ['sum']
        }).reset_index()
        df_grouped.columns = ['YearSeason', 'Global Type', 'Value_sum', 'Value_avg', 'Guests_sum', 'Guests_avg', 'Quantity_sum', 'Quantity_avg'] if show_average else ['YearSeason', 'Global Type', 'Value_sum', 'Guests_sum', 'Quantity_sum']
    else:
        df_grouped = df_filtered.groupby('YearSeason').agg({
            'Value': ['sum', 'mean'] if show_average else ['sum'],
            'Guests': ['sum', 'mean'] if show_average else ['sum'],
            'Quantity': ['sum', 'mean'] if show_average else ['sum']
        }).reset_index()
        df_grouped.columns = ['YearSeason', 'Value_sum', 'Value_avg', 'Guests_sum', 'Guests_avg', 'Quantity_sum', 'Quantity_avg'] if show_average else ['YearSeason', 'Value_sum', 'Guests_sum', 'Quantity_sum']

    # Check if df_grouped is not None and not empty before plotting
    if df_grouped.empty:
        st.error("No data found to display.")
    else:
        # Display summary statistics
        with summary_tab:
            st.write("Summary Statistics")
            st.write(df[['Value', 'Guests', 'Quantity']].describe())

        # Visualization using plotly.graph_objects
        with value_chart_tab:
            fig1 = go.Figure()
            fig2 = go.Figure()
            fig3 = go.Figure()

            if filter_option != "None" and filter_values:
                for value in filter_values:
                    filtered_df = df_grouped[df_grouped[filter_option] == value]
                    fig1.add_trace(go.Scatter(x=filtered_df["YearSeason"], y=filtered_df["Value_sum"], mode='lines+markers', name=f'Transaction Value Sum - {value}', hovertemplate='%{x}<br>Value Sum: %{y}<br>Category: ' + value))
                    if show_average:
                        fig1.add_trace(go.Scatter(x=filtered_df["YearSeason"], y=filtered_df["Value_avg"], mode='lines+markers', name=f'Monthly Average Transaction Value - {value}', line=dict(dash='dot'), hovertemplate='%{x}<br>Average Value: %{y}<br>Category: ' + value))

                    fig2.add_trace(go.Scatter(x=filtered_df["YearSeason"], y=filtered_df["Guests_sum"], mode='lines+markers', name=f'Guests Sum - {value}', hovertemplate='%{x}<br>Guests Sum: %{y}<br>Category: ' + value))
                    if show_average:
                        fig2.add_trace(go.Scatter(x=filtered_df["YearSeason"], y=filtered_df["Guests_avg"], mode='lines+markers', name=f'Monthly Average Guests - {value}', line=dict(dash='dot'), hovertemplate='%{x}<br>Average Guests: %{y}<br>Category: ' + value))

                    fig3.add_trace(go.Scatter(x=filtered_df["YearSeason"], y=filtered_df["Quantity_sum"], mode='lines+markers', name=f'Quantity Sum - {value}', hovertemplate='%{x}<br>Quantity Sum: %{y}<br>Category: ' + value))
                    if show_average:
                        fig3.add_trace(go.Scatter(x=filtered_df["YearSeason"], y=filtered_df["Quantity_avg"], mode='lines+markers', name=f'Monthly Average Quantity - {value}', line=dict(dash='dot'), hovertemplate='%{x}<br>Average Quantity: %{y}<br>Category: ' + value))
            else:
                fig1.add_trace(go.Scatter(x=df_grouped["YearSeason"], y=df_grouped["Value_sum"], mode='lines+markers', name='Transaction Value Sum', hovertemplate='%{x}<br>Value Sum: %{y}'))
                if show_average:
                    fig1.add_trace(go.Scatter(x=df_grouped["YearSeason"], y=df_grouped["Value_avg"], mode='lines+markers', name='Monthly Average Transaction Value', line=dict(dash='dot'), hovertemplate='%{x}<br>Average Value: %{y}'))

                fig2.add_trace(go.Scatter(x=df_grouped["YearSeason"], y=df_grouped["Guests_sum"], mode='lines+markers', name='Guests Sum', hovertemplate='%{x}<br>Guests Sum: %{y}'))
                if show_average:
                    fig2.add_trace(go.Scatter(x=df_grouped["YearSeason"], y=df_grouped["Guests_avg"], mode='lines+markers', name='Monthly Average Guests', line=dict(dash='dot'), hovertemplate='%{x}<br>Average Guests: %{y}'))

                fig3.add_trace(go.Scatter(x=df_grouped["YearSeason"], y=df_grouped["Quantity_sum"], mode='lines+markers', name='Quantity Sum', hovertemplate='%{x}<br>Quantity Sum: %{y}'))
                if show_average:
                    fig3.add_trace(go.Scatter(x=df_grouped["YearSeason"], y=df_grouped["Quantity_avg"], mode='lines+markers', name='Monthly Average Quantity', line=dict(dash='dot'), hovertemplate='%{x}<br>Average Quantity: %{y}'))

            fig1.update_layout(height=400, title="Market Value Analysis by Season", xaxis_title="Season", yaxis_title="Transaction Value", xaxis=dict(type='category', categoryorder='category ascending'))
            fig2.update_layout(height=400, title="Guest Analysis by Season", xaxis_title="Season", yaxis_title="Guests", xaxis=dict(type='category', categoryorder='category ascending'))
            fig3.update_layout(height=400, title="Quantity Analysis by Season", xaxis_title="Season", yaxis_title="Quantity", xaxis=dict(type='category', categoryorder='category ascending'))

            st.plotly_chart(fig1, use_container_width=True)
            st.plotly_chart(fig2, use_container_width=True)
            st.plotly_chart(fig3, use_container_width=True)

        # Display grouped DataFrame in another container
        with value_dataframe_tab:
            st.write("Transaction Value Data")
            transaction_value_columns = ['YearSeason']
            if filter_option == "Venue Name":
                transaction_value_columns.append('Venue Name')
            elif filter_option == "Venue Type":
                transaction_value_columns.append('Venue Type')
            elif filter_option == "Global Type":
                transaction_value_columns.append('Global Type')
            transaction_value_columns += ['Value_sum']
            if show_average:
                transaction_value_columns.append('Value_avg')
            st.dataframe(df_grouped[transaction_value_columns], height=400, width=1000)
            csv_transaction_value = convert_df_to_csv(df_grouped[transaction_value_columns])
            st.download_button(label="Download Transaction Value Data as CSV", data=csv_transaction_value, file_name='transaction_value_data.csv', mime='text/csv')

            st.write("Guest Data")
            guest_columns = ['YearSeason']
            if filter_option == "Venue Name":
                guest_columns.append('Venue Name')
            elif filter_option == "Venue Type":
                guest_columns.append('Venue Type')
            elif filter_option == "Global Type":
                guest_columns.append('Global Type')
            guest_columns += ['Guests_sum']
            if show_average:
                guest_columns.append('Guests_avg')
            st.dataframe(df_grouped[guest_columns], height=400, width=1000)
            csv_guests = convert_df_to_csv(df_grouped[guest_columns])
            st.download_button(label="Download Guest Data as CSV", data=csv_guests, file_name='guest_data.csv', mime='text/csv')

            st.write("Quantity Data")
            quantity_columns = ['YearSeason']
            if filter_option == "Venue Name":
                quantity_columns.append('Venue Name')
            elif filter_option == "Venue Type":
                quantity_columns.append('Venue Type')
            elif filter_option == "Global Type":
                quantity_columns.append('Global Type')
            quantity_columns += ['Quantity_sum']
            if show_average:
                quantity_columns.append('Quantity_avg')
            st.dataframe(df_grouped[quantity_columns], height=400, width=1000)
            csv_quantity = convert_df_to_csv(df_grouped[quantity_columns])
            st.download_button(label="Download Quantity Data as CSV", data=csv_quantity, file_name='quantity_data.csv', mime='text/csv')
else:
    st.error("Failed to retrieve data.")
