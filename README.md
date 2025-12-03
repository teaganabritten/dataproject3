# Global Terrorism Database (GTD) ETL and Analysis Pipeline
## Overview
This project implements a complete ETL and analytical workflow for the Global Terrorism Database (GTD). The pipeline is orchestrated with Prefect and includes raw data ingestion, cleaning, transformation, storage in DuckDB, and generation of analytical visualizations.
A separate Streamlit dashboard is provided for interactive exploration using a smaller sample of the dataset for faster performance.
In addition to the historical dataset, the project incorporates a real-time component using the NewsAPI. A Kafka producer periodically retrieves current news articles related to terrorism, while a Kafka consumer streams these articles into DuckDB for storage and analysis. This allows for a meaningful comparison between long-term, data-driven terrorism trends (GTD) and present-day media coverage patterns captured from live news feeds.

## Data Source
### Global Terrorism Database (GTD):
https://www.start.umd.edu/data-tools/GTD
Codebook (under “GTD Codebook”):
https://www.start.umd.edu/using-gtd
### Variables Used
eventid, iyear, imonth, iday, country_txt, region_txt, provstate, city,
latitude, longitude, attacktype1_txt, targtype1_txt, weaptype1_txt, gname,
nkill, nwound, success, suicide, multiple, individual, summary
Project Structure
*Note: Please run the prefect_gtd.py file first then the gtd_dashboard.py
## 1. Prefect ETL and Analysis (prefect_gtd.py)
This script executes the full ETL pipeline on the complete GTD dataset (approximately 200,000 rows).
It includes the following components:

• Loading the raw GTD CSV into DuckDB

• Cleaning and transforming the dataset (type casting, filtering invalid coordinates, removing malformed entries)

• Storing a cleaned, analysis-ready table in DuckDB for fast OLAP-style queries

• Running analytical SQL queries to compute trends, distributions, group frequencies, and regional patterns

• Generating multiple static plots and saving them as PNG files for reporting

DuckDB is used due to its high performance for analytical workloads and its ability to handle large datasets efficiently without requiring a full database server.

## 2. Streamlit Dashboard (gtd_dashboard.py)
A Streamlit dashboard was created using a randomly sampled subset of 20,000 records.
This subset enables:

• Fast loading and smooth interaction

• Real-time filtering and visualization

• A lightweight demonstration of the full dataset’s analytical capabilities

The dashboard includes maps, charts, filters, KPIs, and interactive exploratory tools.
## 3. NewsAPI Streaming and Analysis (newsdataanalysis.py)
This component integrates real-time news data into the project.
Key elements include:

• Periodic collection of live news articles related to terrorism using the NewsAPI

• A Kafka-based producer that fetches articles multiple times per day and streams them into a Kafka topic

• A Kafka consumer that inserts streamed news data into DuckDB for storage

• NLP-based analysis of titles and descriptions to extract key terms, recurring patterns, and thematic clusters

• Visualizations generated from the news data, such as word clouds, source distributions, and keyword heatmaps

• Docker used to containerize the Kafka environment and ensure reproducible producer/consumer execution

This real-time component allows comparison between historical terrorism patterns (GTD) and current media reporting.
## Visualizations Generated
The pipeline produces a range of analytical figures, including:

• Time series for incident counts and fatalities

• Attack-type distributions and regional breakdowns

• Country-level and group-level comparisons

• Heatmap of attack-type versus target-type co-occurrence

• KDE-based global density map of attack locations

• Bubble charts showing the temporal evolution of attack patterns

• Word cloud summarizing attack summaries from GTD

• News article title word cloud

• Bar chart of the top news sources

• Heatmap of keyword frequencies in news article titles

## Streamlit Dashboard: https://teaganabritten-dataproject3-gtd-dashboard-vbrkcv.streamlit.app/


