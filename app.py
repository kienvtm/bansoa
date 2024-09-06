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
cwd = Path(__file__).parent / 'data' / 'daily'

# daily file
dta_daily_path = cwd.joinpath('*.parquet')


db = duckdb.connect()
db.execute(f"CREATE VIEW data_daily AS SELECT * FROM read_parquet('{dta_daily_path}', union_by_name=True)")
# db.execute(fr"create or replace temp view data_daily as select * from read_parquet('C:\Users\kienv\bansoa\data\daily\*.parquet', union_by_name=True)")

@st.cache_data
def get_data(select_date):
    query = rf'''
    SELECT 
        *
    FROM data_daily
    WHERE report_date='{select_date}'
    '''
    dta_mtd_actual = db.execute(query).fetch_df()
    return dta_mtd_actual

@st.cache_data
def get_data_monthly(select_date):
    query = rf'''
    SELECT 
        *
    FROM data_daily a 
    WHERE date_trunc('month',report_date)=date_trunc('month','{select_date}'::date)
    '''
    dta_mtd_actual = db.execute(query).fetch_df()
    return dta_mtd_actual

@st.cache_data
def get_data_daily_summary(select_date):
    query = rf'''
    SELECT 
        report_date
        , sum(Total) Total
    FROM data_daily a 
    WHERE date_trunc('month',report_date)=date_trunc('month','{select_date}'::date)
    and report_date<='{select_date}'::date
    group by
        report_date
    '''
    # st.write(query)
    dta_mtd_actual = db.execute(query).fetch_df()
    return dta_mtd_actual

# Function to generate random color
def random_color():
    return '#{:02x}{:02x}{:02x}'.format(random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))

def chart_burpee_target(data):

    data = data.sort_values(by="percentage_mtd_actualplete")
    # Create the horizontal bar chart
    fig = go.Figure()
    # Add bars for each store
    fig.add_trace(go.Bar(
        x=data['percentage_mtd_actualplete'],  # The percentage mtd_actualplete is shown in the bar
        y=data['user'],  # Store names
        orientation='h',  # Horizontal bar chart
        marker=dict(color=data['color']),  # Assign random colors to the bars
        text=[f"{row['user']} | <b>{row['percentage_mtd_actualplete']:,.0f}%</b> | Target: {row['Target']:,.0f}<br>Thực hiện: <b>{row['mtd_actual']:,.0f}</b> | Difference: {row['difference']:,.0f}"
            for _, row in data.iterrows()],  # Custom data labels
        textposition='outside',  # Position text at the right edge of the bar
        texttemplate='%{text}',  # Format to use custom text
        textfont=dict(size=14),  # Make the text size bigger
        hoverinfo='none'  # Disable hover info to avoid duplication with text labels
        
    ))

    # Customize layout
    fig.update_layout(
        title=f"Thực hiện target tháng {select_date.strftime('%m/%Y')}",
        # xaxis_title="Percentage mtd_actualplete",
        yaxis=dict(showticklabels=False),  # Hide the store names on the y-axis
        xaxis=dict(range=[0, data['percentage_mtd_actualplete'].max()*1.8]),  # Limit x-axis to 100% max
        height=len(data)*60,
        bargap=0.2,  # Increase this to make bars thinner
        bargroupgap=0.05,  # Space between bar groups
        showlegend=False
    )
    return fig

def chart_daily(data):
    # Create the horizontal bar chart
    data = data.sort_values(by="mtd_flg_daily")
    fig = go.Figure()

    # Add bars for each store
    fig.add_trace(go.Bar(
        x=data['mtd_flg_daily'],  # The percentage mtd_actualplete is shown in the bar
        y=data['user'],  # Store names
        orientation='h',  # Horizontal bar chart
        marker=dict(color=data['color']),  # Assign random colors to the bars
        text=[f"{row['user']} | <b>{row['mtd_flg_daily']:,.0f}/{row['mtd_flg_workout']:,.0f}</b> ngày"
            for _, row in data.iterrows()],  # Custom data labels
        textposition='outside',  # Position text at the right edge of the bar
        texttemplate='%{text}',  # Format to use custom text
        hoverinfo='none'  # Disable hover info to avoid duplication with text labels
        
    ))

    # Customize layout
    fig.update_layout(
        title="Số ngày đạt target daily",
        # xaxis_title="Percentage mtd_actualplete",
        yaxis=dict(showticklabels=False),  # Hide the store names on the y-axis
        xaxis=dict(range=[0, dta_chart['mtd_flg_daily'].max()*1.5]),  # Limit x-axis to 100% max
        height=len(dta_chart)*60,
        bargap=0.2,  # Increase this to make bars thinner
        bargroupgap=0.05,  # Space between bar groups
        showlegend=False
    )
    return fig

def summary_kpi(dta_daily_summary):
    fig_total = go.Figure(go.Indicator(
        # mode = "number+delta",
        mode = "number",
        number= {'font': {'color': 'red'}, 'valueformat':',.0f'}, 
        value = dta_daily_summary['Total'].sum(),
        # delta = {"reference": 512, "valueformat": ".0f"},
        title = {"text": "Total Burpee", 'font': {'color': 'red'}},
        domain = {'y': [0, 1], 'x': [0.25, 0.75]}))

    fig_total.add_trace(go.Bar(
        y = dta_daily_summary['Total'],
        x = dta_daily_summary['report_date'],
        hovertemplate='<b>%{x}</b>: %{y:,.0f} VND',
        # text=dta_daily_summary['mtd_actual'],
        textposition='auto'
    ))
    return fig_total

tab1, tab2 = st.tabs(['Summary Report','Individual Report'])

# Get the current date
current_date = datetime.datetime.now()

with tab1:
    col1, col2, col3, col4 = st.columns(4)
    # Get the start of the current month
    with col1:
        start_of_month = current_date.replace(day=1)

        select_date = st.date_input('Select date', value=current_date)



dta_chart = get_data(select_date)
# Calculate percentage mtd_actualplete and differences
dta_chart['percentage_mtd_actualplete'] = (dta_chart['mtd_actual'] / dta_chart['Target']) * 100
dta_chart['difference'] = dta_chart['mtd_actual'] - dta_chart['Target']
# Generate random colors for each store
dta_chart['color'] = [random_color() for _ in dta_chart['user']]
dta_chart.sort_values(by='percentage_mtd_actualplete', ascending=True, inplace=True)
# st.dataframe(dta_chart)

dta_daily_summary = get_data_daily_summary(select_date)
# st.dataframe(dta_daily_summary)


# Show the figure



with tab1:
    total_target = dta_chart['Target'].sum()
    total_actual = dta_chart['mtd_actual'].sum()
    burpee = dta_chart['mtd_Burpee'].sum()
    core = dta_chart['mtd_Core'].sum()
    pushup = dta_chart['mtd_Pushup'].sum()
    run = dta_chart['mtd_Run'].sum()
    squat = dta_chart['mtd_Squat'].sum()
    # with st.container(border=True):
    col10, col20 = st.columns(2)
    with col10:
        with st.container(border=True):
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Target burpee", f"{total_target:,.0f}")
            col2.metric("Đã thực hiện", f"{total_actual:,.0f}")
            col3.metric("Tỷ lệ hoàn thành", f"{total_actual/total_target:,.1%}")
        with st.container(border=True):
            col1, col2, col3= st.columns(3)
            col1.metric("Burpee", f"{burpee:,.0f}")
            col2.metric("Pushup", f"{pushup:,.0f}")
            col3.metric("Core", f"{core:,.0f}")
            col1.metric("Squat", f"{squat:,.0f}")
            col2.metric("Run", f"{run:,.1f}km")
    with col20:
        with st.container(border=True):
            st.plotly_chart(summary_kpi(dta_daily_summary))


    col1, col2 = st.columns(2)
    with col1:
        with st.container(border=True):
            st.plotly_chart(chart_burpee_target(dta_chart))
    with col2:
        with st.container(border=True):
            st.plotly_chart(chart_daily(dta_chart))

with tab2:
    st.write('Đang phát triển')