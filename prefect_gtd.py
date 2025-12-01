"""
DP3 GTD Historical Data Analysis 
Steps: 
1. Task 1: Loading raw csv (200k+ entries)
2. Task 2: Clean the Data + CAST necessary data types
3. Task 3: Data Analysis + Export plots to images
"""

# Import packages
from prefect import flow, task, get_run_logger
import duckdb
import matplotlib.pyplot as plt
import seaborn as sns
import os

sns.set(style="whitegrid")

# plot directory 
os.makedirs("plots", exist_ok=True)

# Task 1: Load Raw CSV
@task
def load_gtd_raw():
    """
    Loads the raw GTD CSV into a DuckDB table
    """
    logger = get_run_logger()
    logger.info("Starting the Loading Task...")

    try:
        conn = duckdb.connect("gtd.duckdb")

        conn.execute("DROP TABLE IF EXISTS gtd_raw")
        logger.info("Loading CSV into DuckDB...")

        conn.execute("""
            CREATE TABLE gtd_raw AS
            SELECT
                eventid,
                iyear,
                imonth,
                iday,
                country_txt,
                region_txt,
                provstate,
                city,
                latitude,
                longitude,
                attacktype1_txt,
                targtype1_txt,
                weaptype1_txt,
                gname,
                nkill,
                nwound,
                success,
                suicide,
                multiple,
                individual,
                summary
            FROM read_csv_auto('globalterrorismdb_0522dist.csv', ignore_errors=true, all_varchar=true);
        """)

        count_check = conn.execute("SELECT COUNT(*) FROM gtd_raw").fetchone()[0]
        logger.info(f"Load step completed — {count_check:,} rows loaded.")
        conn.close()
        return True

    except Exception as e:
        logger.error(f"Loading Failed: {e}")
        raise

# Task 2: Clean & CAST
@task
def clean_data():
    """
    Typecasting 
    Removes rows missing lat/long
    Produces clean, analysis-ready table
    """
    logger = get_run_logger()
    logger.info("Starting Cleaning Task...")

    try:
        conn = duckdb.connect("gtd.duckdb")
        conn.execute("DROP TABLE IF EXISTS gtd_clean")

        logger.info("Cleaning and Transforming Data...")

        #Making sure to alias after typecasting
        conn.execute("""
            CREATE TABLE gtd_clean AS
            SELECT
                eventid::BIGINT AS eventid,
                iyear::INTEGER AS iyear,
                imonth::INTEGER AS imonth,
                iday::INTEGER AS iday,
                country_txt,
                region_txt,
                provstate,
                city,
                latitude::DOUBLE AS latitude,
                longitude::DOUBLE AS longitude,
                attacktype1_txt,
                targtype1_txt,
                weaptype1_txt,
                gname,
                nkill::INTEGER AS nkill,
                nwound::INTEGER AS nwound,
                success::INTEGER AS success,
                suicide::INTEGER AS suicide,
                summary
            FROM gtd_raw
            WHERE latitude IS NOT NULL
              AND longitude IS NOT NULL;
        """)

        count = conn.execute("SELECT COUNT(*) FROM gtd_clean").fetchone()[0]
        logger.info(f"Cleaning Step complete — {count:,} rows kept.")
        conn.close()
        return True

    except Exception as e:
        logger.error(f"Cleaning step failed: {e}")
        raise

# Task 3: Analysis + Graph Export
@task
def analyze_data():
    """
    Generates Visualization 
    - Time series (incidents & fatalities)
    - Categorical breakdowns (attack types, regions, countries)
    - Terrorist-group lethality analysis
    - Attack-type × target-type co-occurrence heatmap
    - KDE-based geospatial density map
    - Bubble chart showing temporal evolution of attack types
    - Word cloud summarizing textual attack summaries
    """
    logger = get_run_logger()
    logger.info("Starting the Analysis step...")

    try:
        conn = duckdb.connect("gtd.duckdb")

        # Time Series: Incidents Per Year
        df_year = conn.execute("""
            SELECT iyear, COUNT(*) AS count
            FROM gtd_clean
            GROUP BY iyear
            ORDER BY iyear
        """).df()

        plt.figure(figsize=(10,5))
        plt.plot(df_year["iyear"], df_year["count"], marker='o')
        plt.title("Incidents Over Time (1970–2020)")
        plt.xlabel("Year")
        plt.ylabel("# Incidents")
        plt.tight_layout()
        plt.savefig("plots/incidents_over_time.png")
        plt.close()

        # Time Series: Fatalities
        df_fatal = conn.execute("""
            SELECT iyear, SUM(COALESCE(nkill,0)) AS fatalities
            FROM gtd_clean
            GROUP BY iyear
            ORDER BY iyear
        """).df()

        plt.figure(figsize=(10,5))
        plt.plot(df_fatal["iyear"], df_fatal["fatalities"])
        plt.title("Fatalities Over Time")
        plt.xlabel("Year")
        plt.ylabel("Fatalities")
        plt.tight_layout()
        plt.savefig("plots/fatalities_over_time.png")
        plt.close()

        # Attack Type Distribution
        df_attack = conn.execute("""
            SELECT attacktype1_txt AS attack, COUNT(*) AS count
            FROM gtd_clean
            GROUP BY attack
            ORDER BY count DESC
        """).df()

        plt.figure(figsize=(10,6))
        sns.barplot(y="attack", x="count", data=df_attack)
        plt.title("Most Common Attack Types")
        plt.tight_layout()
        plt.savefig("plots/attack_type_breakdown.png")
        plt.close()

        # Regional Breakdown
        df_region = conn.execute("""
            SELECT region_txt AS region, COUNT(*) AS count
            FROM gtd_clean
            GROUP BY region
            ORDER BY count DESC
        """).df()

        plt.figure(figsize=(10,6))
        sns.barplot(y="region", x="count", data=df_region)
        plt.title("Incidents by Region")
        plt.tight_layout()
        plt.savefig("plots/incidents_by_region.png")
        plt.close()

        # Top 15 Countries
        df_country = conn.execute("""
            SELECT country_txt AS country, COUNT(*) AS count
            FROM gtd_clean
            GROUP BY country
            ORDER BY count DESC
            LIMIT 15
        """).df()

        plt.figure(figsize=(12,6))
        sns.barplot(y="country", x="count", data=df_country)
        plt.title("Top 15 Most Affected Countries")
        plt.tight_layout()
        plt.savefig("plots/top_countries.png")
        plt.close()

        # Danger Terrorist Groups
        df_groups = conn.execute("""
            SELECT gname AS group_name, SUM(COALESCE(nkill,0)) AS kills
            FROM gtd_clean
            WHERE gname != 'Unknown'
            GROUP BY group_name
            ORDER BY kills DESC
            LIMIT 15
        """).df()

        plt.figure(figsize=(12,6))
        sns.barplot(y="group_name", x="kills", data=df_groups)
        plt.title("Deadliest Terrorist Groups")
        plt.tight_layout()
        plt.savefig("plots/deadliest_groups.png")
        plt.close()

        # Attack × Target Heatmap
        df_attack_target = conn.execute("""
            SELECT attacktype1_txt AS attack, targtype1_txt AS target, COUNT(*) AS count
            FROM gtd_clean
            GROUP BY attack, target
        """).df()

        pivot_table = df_attack_target.pivot(index="attack", columns="target", values="count").fillna(0)

        plt.figure(figsize=(12,10))
        sns.heatmap(pivot_table, cmap="YlGnBu", linewidths=0.5)
        plt.title("Attack Type vs. Target Type Co-Occurrence")
        plt.tight_layout()
        plt.savefig("plots/attack_target_heatmap.png")
        plt.close()

        # Geographic KDE Heatmap
        df_loc = conn.execute("""
            SELECT latitude, longitude
            FROM gtd_clean
            LIMIT 200000
        """).df()

        plt.figure(figsize=(10,6))
        sns.kdeplot(
            x=df_loc["longitude"],
            y=df_loc["latitude"],
            fill=True,
            thresh=0.05
        )
        plt.title("Heatmap of Global Terrorism Locations")
        plt.tight_layout()
        plt.savefig("plots/global_heatmap.png")
        plt.close()

        # Bubble Chart: Attack Type Across Time    
        df_attack_year= conn.execute("""
            SELECT iyear, attacktype1_txt AS attack, 
                   COUNT(*) AS incidents, 
                   SUM(COALESCE(nkill,0)) AS fatalities
            FROM gtd_clean
            GROUP BY iyear, attack
            HAVING COUNT(*) > 0
            ORDER BY iyear
        """).df()                       
        plt.figure(figsize=(14,8))
        scatter = plt.scatter(
            x=df_attack_year['iyear'],
            y=df_attack_year['attack'],
            s=df_attack_year['fatalities']*0.5,  # Bubble size proportional to fatalities
            c=df_attack_year['incidents'],       # Color = frequency
            cmap='Reds',
            alpha=0.7,
            edgecolors='k',
            linewidth=0.5
        )

        plt.colorbar(scatter, label="Number of Incidents")
        plt.xlabel("Year")
        plt.ylabel("Attack Type")
        plt.title("Attack Type Trends Over Time: Frequency (color) vs. Fatalities (size)")
        plt.tight_layout()
        plt.savefig("plots/attack_type_trends_bubble.png")
        plt.close()


        # Word Cloud: Attack Summaries
        from wordcloud import WordCloud

        df_summary = conn.execute("""
            SELECT summary
            FROM gtd_clean
            WHERE summary IS NOT NULL
            LIMIT 50000
        """).df()
        text = " ".join(df_summary['summary'].astype(str).tolist())
        wordcloud = WordCloud(width=1200, height=600, background_color='white', max_words=200).generate(text)
        plt.figure(figsize=(14,8))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis("off")
        plt.title("Common Words in Attack Summaries")
        plt.tight_layout()
        plt.savefig("plots/attack_summary_wordcloud.png")
        plt.close()

        conn.close()
        logger.info("Analysis Complete")
        return True

    except Exception as e:
        logger.error(f"Analysis Failed: {e}")
        raise

# Main Flow
@flow
def gtd_flow():
    """
    1. Load raw data
    2. Clean and prepare analytical dataset
    3. Execute all analyses and write plots to plots folder
    """
    load_gtd_raw()
    clean_data()
    analyze_data()

if __name__ == "__main__":
    gtd_flow()
