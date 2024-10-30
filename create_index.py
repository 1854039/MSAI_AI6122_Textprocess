import argparse
import os
import time
import pandas as pd
import matplotlib.pyplot as plt
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID, NUMERIC, BOOLEAN
from whoosh.analysis import StemmingAnalyzer
import nltk

# Download stopwords
nltk.download('stopwords')


# Preprocess text function
def preprocess_text(text):
    if pd.isna(text):
        return ''
    return text.lower()


# Define the schema for the business index
schema = Schema(
    business_id=ID(stored=True),
    name=TEXT(analyzer=StemmingAnalyzer(), stored=True),
    address=TEXT(analyzer=StemmingAnalyzer(), stored=True),
    city=TEXT(analyzer=StemmingAnalyzer(), stored=True),
    state=TEXT(analyzer=StemmingAnalyzer(), stored=True),
    postal_code=TEXT(analyzer=StemmingAnalyzer(), stored=True),
    latitude=NUMERIC(stored=True),
    longitude=NUMERIC(stored=True),
    stars=NUMERIC(stored=True),
    review_count=NUMERIC(stored=True),
    is_open=BOOLEAN(stored=True),
    attributes=TEXT(analyzer=StemmingAnalyzer(), stored=True),
    categories=TEXT(analyzer=StemmingAnalyzer(), stored=True),
    hours=TEXT(analyzer=StemmingAnalyzer(), stored=True),
)


# Create business index function
def create_business_index():
    if not os.path.exists("indexdir"):
        os.mkdir("indexdir")
        ix = create_in("indexdir", schema)
        print("Business index created successfully!")
        return ix
    else:
        ix = open_dir("indexdir")
        print("Business index opened successfully!")
        return ix


# Index business data function
# Index business data function
def index_businesses(ix, business_df):
    writer = ix.writer()
    total_count = len(business_df)
    business_times = []  # List to capture indexing times
    start_time = time.time()

    for index, row in business_df.iterrows():
        writer.add_document(
            business_id=row["business_id"] if pd.notna(row["business_id"]) else '',
            name=preprocess_text(row["name"]),
            address=preprocess_text(row["address"]),
            city=preprocess_text(row["city"]),
            state=preprocess_text(row["state"]),
            postal_code=preprocess_text(row["postal_code"]),
            latitude=float(row["latitude"]) if pd.notna(row["latitude"]) else 0.0,
            longitude=float(row["longitude"]) if pd.notna(row["longitude"]) else 0.0,
            stars=float(row["stars"]) if pd.notna(row["stars"]) else 0.0,
            review_count=int(row["review_count"]) if pd.notna(row["review_count"]) else 0,
            is_open=bool(row["is_open"]) if pd.notna(row["is_open"]) else False,
            attributes=str(row["attributes"]) if pd.notna(row["attributes"]) else '{}',
            categories=preprocess_text(row["categories"]),
            hours=str(row["hours"]) if pd.notna(row["hours"]) else '{}',
        )

        # Record time every 10%
        if (index + 1) % (total_count // 10) == 0:
            elapsed_time = time.time() - start_time
            business_times.append((index + 1, elapsed_time))  # Capture time for each 10%

    writer.commit()
    print("Businesses indexed successfully!")
    return business_times  # Return the list of times


# Index review data function
def index_reviews(review_ix, review_df):
    writer = review_ix.writer()
    total_count = len(review_df)
    review_times = []  # List to capture indexing times
    start_time = time.time()

    for index, row in review_df.iterrows():
        writer.add_document(
            review_id=row['review_id'],
            user_id=row['user_id'],
            business_id=row['business_id'],
            stars=row['stars'],
            useful=row['useful'],
            funny=row['funny'],
            cool=row['cool'],
            text=preprocess_text(row['text']),
            date=row['date']
        )

        # Record time every 10%
        if (index + 1) % (total_count // 10) == 0:
            elapsed_time = time.time() - start_time
            review_times.append((index + 1, elapsed_time))  # Capture time for each 10%

    writer.commit()
    print("Reviews indexed successfully!")
    return review_times  # Return the list of times

# Define the schema for the review index
review_schema = Schema(
    review_id=ID(stored=True),
    user_id=ID(stored=True),
    business_id=ID(stored=True),
    stars=NUMERIC(stored=True),
    useful=NUMERIC(stored=True),
    funny=NUMERIC(stored=True),
    cool=NUMERIC(stored=True),
    text=TEXT(analyzer=StemmingAnalyzer(), stored=True),
    date=TEXT(stored=True),
)


# Create review index function
def create_review_index():
    if not os.path.exists("review_indexdir"):
        os.mkdir("review_indexdir")
        review_ix = create_in("review_indexdir", review_schema)
        print("Review index created successfully!")
        return review_ix
    else:
        review_ix = open_dir("review_indexdir")
        print("Review index opened successfully!")
        return review_ix

# Index review data function
def index_reviews(review_ix, review_df):
    writer = review_ix.writer()
    total_count = len(review_df)
    review_times = []  # List to capture indexing times
    start_time = time.time()

    for index, row in review_df.iterrows():
        writer.add_document(
            review_id=row['review_id'],
            user_id=row['user_id'],
            business_id=row['business_id'],
            stars=row['stars'],
            useful=row['useful'],
            funny=row['funny'],
            cool=row['cool'],
            text=preprocess_text(row['text']),
            date=row['date']
        )

        # Record time every 10%
        if (index + 1) % (total_count // 10) == 0:
            elapsed_time = time.time() - start_time
            review_times.append((index + 1, elapsed_time))  # Capture time for each 10%

    writer.commit()
    print("Reviews indexed successfully!")
    return review_times  # Return the list of times


# Merge time monitoring data
def merge_times(business_times, review_times):
    merged_times = []
    business_docs, business_elapsed = zip(*business_times) if business_times else ([], [])
    review_docs, review_elapsed = zip(*review_times) if review_times else ([], [])
    all_docs = sorted(set(business_docs).union(review_docs))

    for doc in all_docs:
        business_time = business_elapsed[business_docs.index(doc)] if doc in business_docs else None
        review_time = review_elapsed[review_docs.index(doc)] if doc in review_docs else None
        merged_times.append((doc, business_time, review_time))

    return merged_times


# Plot combined time monitoring data
def plot_combined_time_monitor(merged_times):
    if not merged_times:
        print("No data to plot for combined indexing times.")
        return

    docs = [doc for doc, _, _ in merged_times]
    business_times = [time if time is not None else 0 for _, time, _ in merged_times]
    review_times = [time if time is not None else 0 for _, _, time in merged_times]

    plt.figure(figsize=(12, 6))
    plt.plot(docs, business_times, marker='o', label='Business Indexing Time', color='blue')
    plt.plot(docs, review_times, marker='x', label='Review Indexing Time', color='orange')
    plt.title('Combined Indexing Time Monitor')
    plt.xlabel('Number of Documents Indexed')
    plt.ylabel('Time (seconds)')
    plt.grid(True)
    plt.legend()
    plt.savefig('combined_indexing_time.png')
    plt.show()


# Main function
def main(args):
    # Load business and review data from CSV files
    business_df = pd.read_csv(args.business_file)
    review_df = pd.read_csv(args.review_file)

    # Create and index business data
    ix = create_business_index()
    business_times = []
    if not os.path.exists("indexdir/MAIN_WRITELOCK"):
        business_times = index_businesses(ix, business_df)
    else:
        print("Business index already exists; skipping indexing.")

    # Create and index review data
    review_ix = create_review_index()
    review_times = []
    if not os.path.exists("review_indexdir/MAIN_WRITELOCK"):
        review_times = index_reviews(review_ix, review_df)
    else:
        print("Review index already exists; skipping indexing.")

    # Call merging and plotting functions as needed
    merged_times = merge_times(business_times, review_times)
    plot_combined_time_monitor(merged_times)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Index business and review data.")
    parser.add_argument('--business_file', required=True, help="Path to the business CSV file.")
    parser.add_argument('--review_file', required=True, help="Path to the review CSV file.")
    args = parser.parse_args()
    main(args)
