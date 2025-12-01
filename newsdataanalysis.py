import duckdb
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from wordcloud import WordCloud, STOPWORDS

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='newsanalysis.log'
)
logger = logging.getLogger(__name__)

def import_data():
    con = duckdb.connect(database='news.db', read_only=True)
    logger.info("Connected to DuckDB instance for analysis")
    print("Connected to DuckDB instance for analysis")

    df = con.execute("SELECT * FROM articles").fetchdf()
    logger.info(f"Imported {len(df)} records from news_articles table")
    print(f"Imported {len(df)} records from news_articles table")
    
    return df

def wordcloud(df):
    try:
            # generate a word cloud from article titles
        titles = df['title'].dropna().astype(str)
        text = " ".join(titles)

        # customise stopwords (merge default STOPWORDS with some common noisy tokens)
        stopwords = set(STOPWORDS)
        stopwords.update({'news', 'said', 'say', 'new', 'one', 'just', 'will', 'us', 'mr', 'mrs', 'ms', 'also'})

        # generate and display the word cloud
        wc = WordCloud(width=1200, height=600, background_color='white',
                stopwords=stopwords, collocations=False).generate(text)

        plt.figure(figsize=(12, 6))
        plt.imshow(wc, interpolation='bilinear')
        plt.axis('off')
        plt.show()

        #save to file
        wc.to_file("title_wordcloud.png")
        logger.info("Word cloud generated and saved to title_wordcloud.png")
        print("Word cloud generated and saved to title_wordcloud.png")
    except Exception as e:
        logger.error(f"Error generating word cloud: {e}")
        print(f"Error generating word cloud: {e}")

def sources_graph(df):
    try:
        counts = df['source_name'].value_counts()
        top20 = counts.head(20)

        plt.figure(figsize=(10, 7))
        sns.set_theme(style="whitegrid")
        sns.barplot(x=top20.values, y=top20.index, palette="viridis")
        plt.xlabel("Number of articles")
        plt.ylabel("Source")
        plt.title("Top 20 Sources by Article Count")
        for i, v in enumerate(top20.values):
            plt.text(v + max(top20.values) * 0.01, i, str(v), va='center')
        plt.tight_layout()
        plt.savefig("news_sources.png", dpi=150, bbox_inches='tight')
        plt.show()
        logger.info("created source graph and exported file")
        print("created source graph and exported file")
    except Exception as e:
        logger.error(f"Error generating sources graph: {e}")
        print(f"Error generating sources graph: {e}")

def heatmap(df):
    try:
        import re

        # determine top 20 sources from the provided dataframe
        counts = df['source_name'].value_counts()
        top20 = counts.head(20)
        top_sources = list(top20.index)

        # build stopwords set for token filtering
        union_stop = set(STOPWORDS) | {'news', 'said', 'say', 'new', 'one', 'just', 'will', 'us', 'mr', 'mrs', 'ms', 'also'}

        def tokenize_title(t):
            s = str(t).lower()
            s = re.sub(r"[^\w\s']", " ", s)  # keep words and apostrophes
            toks = [w.strip("'") for w in s.split()]
            toks = [w for w in toks if len(w) > 2 and w.isalpha() and w not in union_stop]
            return toks

        # build token dataframe
        tmp = df[['source_name', 'title']].copy()
        tmp['tokens'] = tmp['title'].apply(tokenize_title)
        tmp = tmp.explode('tokens').dropna(subset=['tokens'])

        # restrict to top sources and get top keywords overall
        tmp = tmp[tmp['source_name'].isin(top_sources)]

        kw_counts = tmp['tokens'].value_counts()
        top_kws = kw_counts.head(15).index.tolist()

        # pivot to create matrix (keywords x sources)
        mat = tmp[tmp['tokens'].isin(top_kws)].groupby(['tokens', 'source_name']).size().unstack(fill_value=0)
        mat = mat.reindex(top_kws)  # ensure keyword order by frequency

        # heatmap
        plt.figure(figsize=(12, 8))
        sns.heatmap(mat, cmap='YlGnBu', annot=True, fmt='d', linewidths=0.5)
        plt.title("Top keywords in article titles by source (Top 20 sources)")
        plt.xlabel("Source")
        plt.ylabel("Keyword")
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig("keywords_by_source_heatmap.png", dpi=150, bbox_inches='tight')
        plt.show()
        logger.info("created keywords heatmap and exported file")
        print("created keywords heatmap and exported file")
    except Exception as e:
        logger.error(f"Error generating heatmap: {e}")
        print(f"Error generating heatmap: {e}")

def main():
    df = import_data()
    wordcloud(df)
    sources_graph(df)
    heatmap(df)

if __name__ == "__main__":
    main()