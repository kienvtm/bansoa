import streamlit as st
import duckdb
import pandas as pd
import datetime
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import re
import random
st.set_page_config(layout="wide")

st.title('BẠN SỢ À Report')

# data folder path
cwd = Path(__file__).parent.joinpath('data').joinpath('daily')

# daily file
dta_daily_path = cwd.joinpath('*.parquet')


db = duckdb.connect()
db.execute(f"CREATE VIEW data_daily AS SELECT * FROM '{dta_daily_path}'")

@st.cache_data
def get_data(select_date):
    query = rf'''
    SELECT 
        *
    FROM data_daily a 
    WHERE report_date='{select_date}'
    '''
    dta_com = db.execute(query).fetch_df()
    return dta_com

@st.cache_data
def get_data_monthly(select_date):
    query = rf'''
    SELECT 
        *
    FROM data_daily a 
    WHERE date_trunc('month',report_date)=date_trunc('month','{select_date}')
    '''
    dta_com = db.execute(query).fetch_df()
    return dta_com

# Get the current date
current_date = datetime.datetime.now()

# Get the start of the current month
start_of_month = current_date.replace(day=1)
select_date = st.sidebar.date_input('Chon ngay', value=current_date)

dta_chart = get_data(select_date)



# Calculate percentage complete and differences
dta_chart['percentage_complete'] = (dta_chart['mtd_actual'] / dta_chart['Target']) * 100
dta_chart['difference'] = dta_chart['mtd_actual'] - dta_chart['Target']

# Function to generate random color
def random_color():
    return '#{:02x}{:02x}{:02x}'.format(random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))

# Generate random colors for each store
dta_chart['color'] = [random_color() for _ in dta_chart['user']]
dta_chart.sort_values(by='percentage_complete', ascending=True, inplace=True)

def chart_burpee_target(dta_chart):
    # Create the horizontal bar chart
    fig = go.Figure()
    # Add bars for each store
    fig.add_trace(go.Bar(
        x=dta_chart['percentage_complete'],  # The percentage complete is shown in the bar
        y=dta_chart['user'],  # Store names
        orientation='h',  # Horizontal bar chart
        marker=dict(color=dta_chart['color']),  # Assign random colors to the bars
        text=[f"{row['user']} | <b>{row['percentage_complete']:,.0f}%</b> | Target: {row['Target']:,.0f}<br>Thực hiện: <b>{row['mtd_actual']:,.0f}</b> | Difference: {row['difference']:,.0f}"
            for _, row in dta_chart.iterrows()],  # Custom data labels
        textposition='outside',  # Position text at the right edge of the bar
        texttemplate='%{text}',  # Format to use custom text
        textfont=dict(size=14),  # Make the text size bigger
        hoverinfo='none'  # Disable hover info to avoid duplication with text labels
        
    ))

    # Customize layout
    fig.update_layout(
        title=f"Thực hiện target tháng {select_date.strftime('%m/%Y')}",
        # xaxis_title="Percentage Complete",
        yaxis=dict(showticklabels=False),  # Hide the store names on the y-axis
        xaxis=dict(range=[0, dta_chart['percentage_complete'].max()*1.5]),  # Limit x-axis to 100% max
        height=len(dta_chart)*60,
        bargap=0.2,  # Increase this to make bars thinner
        bargroupgap=0.05,  # Space between bar groups
        showlegend=False
    )
    return fig

def chart_daily(dta_chart):
    # Create the horizontal bar chart
    fig = go.Figure()

    # Add bars for each store
    fig.add_trace(go.Bar(
        x=dta_chart['mtd_daily'],  # The percentage complete is shown in the bar
        y=dta_chart['user'],  # Store names
        orientation='h',  # Horizontal bar chart
        marker=dict(color=dta_chart['color']),  # Assign random colors to the bars
        text=[f"{row['user']} | <b>{row['mtd_daily']:,.0f}</b> ngày"
            for _, row in dta_chart.iterrows()],  # Custom data labels
        textposition='outside',  # Position text at the right edge of the bar
        texttemplate='%{text}',  # Format to use custom text
        # hoverinfo='none'  # Disable hover info to avoid duplication with text labels
        
    ))

    # Customize layout
    fig.update_layout(
        title="Số ngày đạt target daily",
        # xaxis_title="Percentage Complete",
        yaxis=dict(showticklabels=False),  # Hide the store names on the y-axis
        xaxis=dict(range=[0, dta_chart['mtd_daily'].max()*1.5]),  # Limit x-axis to 100% max
        height=len(dta_chart)*60,
        bargap=0.2,  # Increase this to make bars thinner
        bargroupgap=0.05,  # Space between bar groups
        showlegend=False
    )
    return fig

# Show the figure
total_target = dta_chart['Target'].sum()
total_actual = dta_chart['mtd_actual'].sum()

col1, col2 = st.columns(2)
with col1:
    with st.container(border=True):
        st.plotly_chart(chart_burpee_target(dta_chart))
with col2:
    with st.container(border=True):
        st.plotly_chart(chart_daily(dta_chart))
