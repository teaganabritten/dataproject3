import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from wordcloud import WordCloud
import matplotlib.pyplot as plt

st.set_page_config(layout="wide", page_title="Global Terrorism Dashboard")

# Database connection
def get_duckdb_connection():
    return duckdb.connect("gtd.duckdb", read_only=True)

con = get_duckdb_connection()

# Helper to get filter options (Referenced CHAT for this code)
def get_filter_options(column, where_clause=""):
    result = con.execute(f"SELECT DISTINCT {column} FROM gtd_clean {where_clause}").fetchall()
    return ["All"] + [r[0] for r in result]

# Sidebar filters
years = con.execute("SELECT MIN(iyear), MAX(iyear) FROM gtd_clean").fetchone()
year_range_slider = st.sidebar.slider(
    "Select Year Range", int(years[0]), int(years[1]), (int(years[0]), int(years[1]))
)

region_filter = st.sidebar.selectbox("Region", get_filter_options("region_txt"))
country_filter = st.sidebar.selectbox(
    "Country",
    get_filter_options("country_txt", f"WHERE region_txt = '{region_filter}'") if region_filter != "All" else get_filter_options("country_txt")
)
attack_filter = st.sidebar.selectbox("Attack Type", get_filter_options("attacktype1_txt"))
group_filter = st.sidebar.selectbox("Terrorist Group", get_filter_options("gname"))

# Load filtered data
def load_filtered_data(year_range, region, country, attack, group, limit=20000):
    query = f"""
        SELECT iyear, country_txt, region_txt, city, latitude, longitude,
               attacktype1_txt, targtype1_txt, gname, nkill, nwound, summary
        FROM gtd_clean
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
          AND iyear BETWEEN {year_range[0]} AND {year_range[1]}
    """
    if region != "All":
        query += f" AND region_txt = '{region}'"
    if country != "All":
        query += f" AND country_txt = '{country}'"
    if attack != "All":
        query += f" AND attacktype1_txt = '{attack}'"
    if group != "All":
        query += f" AND gname = '{group}'"

    # Sampling for representative data
    query += f" ORDER BY RANDOM() LIMIT {limit}"
    return con.execute(query).df()

df = load_filtered_data(year_range_slider, region_filter, country_filter, attack_filter, group_filter)

# Clean & prepare data
if not df.empty:
    df['iyear'] = df['iyear'].astype(int)
    df = df.sort_values('iyear')
    df['iyear_str'] = df['iyear'].astype(str)
    df['nkill_size'] = df['nkill'].fillna(1)
    df['nkill'] = df['nkill'].fillna(0)
    df['nwound'] = df['nwound'].fillna(0)

# KPI Metrics
st.title("Global Terrorism Dashboard")
col1, col2, col3 = st.columns(3)
col1.metric("Total Incidents", len(df))
col2.metric("Total Fatalities", int(df['nkill'].sum()) if not df.empty else 0)
col3.metric("Total Injuries", int(df['nwound'].sum()) if not df.empty else 0)

# Animated Map (Cumulative)
st.subheader("Terrorism Map (Cumulative Animation)")
if not df.empty:
    cumulative_df = pd.DataFrame()
    for year in sorted(df['iyear'].unique()):
        temp = df[df['iyear'] <= year].copy()
        temp['iyear_str'] = str(year)
        cumulative_df = pd.concat([cumulative_df, temp])

    fig_map = px.scatter_mapbox(
        cumulative_df,
        lat="latitude",
        lon="longitude",
        hover_name="city",
        hover_data=["country_txt","attacktype1_txt","gname","nkill","nwound","summary"],
        color="attacktype1_txt",
        size="nkill_size",
        animation_frame="iyear_str",
        zoom=1,
        height=600
    )
    fig_map.update_layout(mapbox_style="carto-positron")
    st.plotly_chart(fig_map, width="stretch")

st.subheader("Incidents Over Time (Cumulative)")
if not df.empty:
    cumulative_df = pd.DataFrame()
    for year in sorted(df['iyear'].unique()):
        temp = df[df['iyear'] <= year].groupby('iyear').size().reset_index(name='count')
        temp['iyear_str'] = temp['iyear'].astype(str)
        temp['frame_year'] = str(year)
        cumulative_df = pd.concat([cumulative_df, temp])

    fig_time_line = px.line(
        cumulative_df,
        x='iyear_str',
        y='count',
        markers=True,
        animation_frame='frame_year',
        labels={'iyear_str':'Year', 'count':'Number of Incidents'},
        title="Cumulative Incidents Over Time"
    )
    fig_time_line.update_layout(xaxis=dict(categoryorder='category ascending'))
    st.plotly_chart(fig_time_line, width="stretch")

# Attack Type Breakdown
st.subheader("Attack Type Breakdown")
if not df.empty:
    attack_count = df['attacktype1_txt'].value_counts().reset_index()
    attack_count.columns = ['Attack Type', 'Count']
    fig_attack = px.bar(
        attack_count,
        x='Attack Type',
        y='Count',
        color='Count',
        color_continuous_scale='Reds',
        title="Attack Type Frequency"
    )
    st.plotly_chart(fig_attack, width="stretch")

# Incidents by Region
st.subheader("Incidents by Region")
if not df.empty:
    region_count = df.groupby('region_txt').size().reset_index(name='count')
    fig_region = px.bar(
        region_count,
        x='region_txt',
        y='count',
        color='count',
        color_continuous_scale='OrRd',
        title="Incidents by Region"
    )
    st.plotly_chart(fig_region, width="stretch")

# Incidents by Country
st.subheader("Incidents by Country")
if not df.empty:
    country_count = df.groupby('country_txt').size().reset_index(name='count')
    fig_country = px.choropleth(
        country_count,
        locations='country_txt',
        locationmode='country names',
        color='count',
        color_continuous_scale='OrRd',
        title="Incidents by Country"
    )
    st.plotly_chart(fig_country, width="stretch")

# Bubble Chart: Attack Trends vs Fatalities
st.subheader("Attack Trends: Frequency vs Fatalities")
if not df.empty:
    df_attack_year = df.groupby(['iyear','attacktype1_txt']).agg(
        incidents=('attacktype1_txt','count'),
        fatalities=('nkill','sum')
    ).reset_index()
    fig_bubble = px.scatter(
        df_attack_year,
        x='iyear',
        y='attacktype1_txt',
        size='fatalities',
        color='incidents',
        color_continuous_scale='Reds',
        size_max=40,
        title="Attack Type Trends Over Time: Size=Fatalities, Color=Incidents"
    )
    st.plotly_chart(fig_bubble, width="stretch")

# Heatmap: Attack Type vs Target Type
st.subheader("Attack Type vs Target Type Heatmap")
if not df.empty:
    pivot_table = df.pivot_table(
        index='attacktype1_txt', columns='targtype1_txt', values='iyear', aggfunc='count', fill_value=0
    )
    fig_heatmap = px.imshow(
        pivot_table,
        text_auto=True,
        aspect="auto",
        color_continuous_scale='YlGnBu',
        title="Attack Type vs Target Type"
    )
    st.plotly_chart(fig_heatmap, width="stretch")

# Word Cloud
st.subheader("Common Words in Attack Summaries")
if not df.empty:
    text = " ".join(df['summary'].dropna().astype(str).tolist())
    wordcloud = WordCloud(width=1200, height=600, background_color='white', max_words=200).generate(text)
    plt.figure(figsize=(14,6))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    st.pyplot(plt)
