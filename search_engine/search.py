import argparse
from datetime import datetime
import nltk
from whoosh.qparser import QueryParser
from create_index import preprocess_text

# Download stopwords for text preprocessing
nltk.download('stopwords')

# Import necessary libraries
import os
import time
import pandas as pd
import matplotlib.pyplot as plt
from whoosh.index import create_in, open_dir

# Define index directories
BUSINESS_INDEX_DIR = "indexdir"
REVIEW_INDEX_DIR = "review_indexdir"

# Open the business and review indices
ix = open_dir(BUSINESS_INDEX_DIR)
review_ix = open_dir(REVIEW_INDEX_DIR)

def search_business(query_str, top_n, sort_by='stars'):
    """
    Search for businesses based on keywords.

    Parameters:
        query_str (str): The search keywords.
        top_n (int): The number of results to return.
        sort_by (str): The sorting method, can be 'stars' or 'name'.

    Returns:
        list: A list of search results.
    """
    query_str = preprocess_text(query_str)  # Preprocess the query
    start_time = time.time()  # Start timing
    with ix.searcher() as searcher:
        query = QueryParser("name", ix.schema).parse(query_str)  # Parse the query

        # Perform the search
        results = searcher.search(query, limit=None, sortedby=sort_by)

        # Collect results
        businesses = [(result['business_id'], result['name'], result['stars']) for result in results]

        # Sort results based on the specified method
        if sort_by == 'stars':
            businesses.sort(key=lambda x: x[2], reverse=True)  # Sort by stars in descending order
        elif sort_by == 'name':
            businesses.sort(key=lambda x: x[1])  # Sort by name in ascending order

    elapsed_time = time.time() - start_time  # End timing
    total_results = len(businesses)  # Get total number of results
    print(f"Search Businesses executed in {elapsed_time:.4f} seconds. Found {total_results} results.")

    # Return the top N results or all if requested exceeds available
    if top_n > total_results:
        print(f"Requested top_n ({top_n}) exceeds available results. Returning all {total_results} results.")
        return businesses  # Return all results
    else:
        return businesses[:top_n]  # Return top N results


def search_reviews(query_str, top_n=10, sort_by='useful'):
    """
    Search for reviews based on keywords.

    Parameters:
        query_str (str): The search keywords.
        top_n (int): The number of results to return (default is 10).
        sort_by (str): The sorting method, can be 'useful' or 'date'.

    Returns:
        list: A list of search results.
    """
    query_str = preprocess_text(query_str)  # Preprocess the query

    start_time = time.time()  # Start timing
    with review_ix.searcher() as searcher:
        query = QueryParser("text", review_ix.schema).parse(query_str)  # Parse the query
        results = searcher.search(query, limit=None, sortedby=sort_by)  # Perform the search

        # Collect results
        reviews = [
            (result['review_id'], result['text'], result['stars'], result['useful'], result['date'])
            for result in results
        ]

        # Sort results based on the specified method
        if sort_by == 'date':
            reviews.sort(key=lambda x: datetime.strptime(x[4], "%Y-%m-%d %H:%M:%S"), reverse=True)  # Sort by date in descending order
        elif sort_by == 'useful':
            reviews.sort(key=lambda x: x[3], reverse=True)  # Sort by useful count in descending order

    elapsed_time = time.time() - start_time  # End timing
    print(f"Search Reviews executed in {elapsed_time:.4f} seconds.")
    return reviews[:top_n]  # Return top N results


def search_geospatial(lat_min, lat_max, lon_min, lon_max, top_n=10, sort_by='stars'):
    """
    Perform geospatial search based on latitude and longitude.

    Parameters:
        lat_min (float): Minimum latitude.
        lat_max (float): Maximum latitude.
        lon_min (float): Minimum longitude.
        lon_max (float): Maximum longitude.
        top_n (int): The number of results to return (default is 10).
        sort_by (str): The sorting method, can be 'stars' or 'name'.

    Returns:
        list: A list of search results.
    """
    start_time = time.time()  # Start timing
    results = []
    epsilon = 1e-7  # Tolerance for latitude and longitude comparisons
    with ix.searcher() as searcher:
        # Iterate over all documents in the index
        for hit in searcher.documents():
            lat = float(hit['latitude'])
            lon = float(hit['longitude'])

            # Check if the document's latitude and longitude fall within the specified range
            if (lat_min - epsilon <= lat <= lat_max + epsilon) and (
                    lon_min - epsilon <= lon <= lon_max + epsilon):
                results.append((hit['business_id'], hit['name'], lat, lon, hit['stars']))

    # Sort results based on the specified method
    if sort_by == 'stars':
        results.sort(key=lambda x: x[4], reverse=True)  # Sort by stars in descending order
    elif sort_by == 'name':
        results.sort(key=lambda x: x[1])  # Sort by name in ascending order

    elapsed_time = time.time() - start_time  # End timing
    print(f"Search Geospatial executed in {elapsed_time:.4f} seconds.")

    # Visualize search results
    plt.figure(figsize=(10, 6))

    # Plot all results' latitude and longitude
    all_lats = [result[2] for result in results]
    all_lons = [result[3] for result in results]

    # Plot all results in blue
    plt.scatter(all_lons, all_lats, color='blue', label='All Results', alpha=0.5)

    # Plot only the top N results in red
    if results:
        top_lats = [result[2] for result in results[:top_n]]
        top_lons = [result[3] for result in results[:top_n]]
        plt.scatter(top_lons, top_lats, color='red', label='Top N Results', alpha=0.7)

    # Draw a rectangle for the search area
    plt.gca().add_patch(plt.Rectangle((lon_min, lat_min), lon_max - lon_min, lat_max - lat_min,
                                        fill=None, edgecolor='blue', linewidth=2, label='Search Area'))

    # Set graph properties
    plt.xlim(-113.7313182, -113.2574946)
    plt.ylim(53.3526529, 53.6791969)
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title('Geospatial Search Results')
    plt.legend()
    plt.grid()
    plt.show()

    return results[:top_n]  # Return top N results

def main(args):
    """
    Main function to execute search commands based on user input.

    Parameters:
        args: Command line arguments parsed by argparse.
    """
    if args.command == 'search_business':
        results = search_business(args.query, args.top_n, args.sort_by)
        for res in results:
            print(f"ID: {res[0]}, Name: {res[1]}, Score: {res[2]}")

    elif args.command == 'search_reviews':
        results = search_reviews(args.query, args.top_n, args.sort_by)
        for res in results:
            print(f"Review ID: {res[0]}, Text: {res[1]}, Score: {res[2]}, Useful: {res[3]}, Date: {res[4]}")

    elif args.command == 'search_geospatial':
        results = search_geospatial(args.lat_min, args.lat_max, args.lon_min, args.lon_max, args.top_n, args.sort_by)
        for res in results:
            print(f"ID: {res[0]}, Name: {res[1]}, Latitude: {res[2]}, Longitude: {res[3]}, Score: {res[4]}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Business Review Search Engine")

    subparsers = parser.add_subparsers(dest="command", help="Subcommands")

    # Search Businesses
    search_business_parser = subparsers.add_parser('search_business', help="Search for businesses")
    search_business_parser.add_argument("query", type=str, help="Keywords to search for businesses")
    search_business_parser.add_argument("--top_n", type=int, default=10, help="Number of results to return (default: 10)")
    search_business_parser.add_argument("--sort_by", type=str, choices=['stars', 'name'], default='stars', help="Sort results by (default: stars)")

    # Search Reviews
    search_reviews_parser = subparsers.add_parser('search_reviews', help="Search for reviews")
    search_reviews_parser.add_argument("query", type=str, help="Keywords to search for reviews")
    search_reviews_parser.add_argument("--top_n", type=int, default=10, help="Number of results to return (default: 10)")
    search_reviews_parser.add_argument("--sort_by", type=str, choices=['useful', 'date'], default='useful', help="Sort results by (default: useful)")

    # Geospatial Search
    search_geospatial_parser = subparsers.add_parser('search_geospatial', help="Perform geospatial search")
    search_geospatial_parser.add_argument("lat_min", type=float, help="Minimum latitude")
    search_geospatial_parser.add_argument("lat_max", type=float, help="Maximum latitude")
    search_geospatial_parser.add_argument("lon_min", type=float, help="Minimum longitude")
    search_geospatial_parser.add_argument("lon_max", type=float, help="Maximum longitude")
    search_geospatial_parser.add_argument("--top_n", type=int, default=10, help="Number of results to return (default: 10)")
    search_geospatial_parser.add_argument("--sort_by", type=str, choices=['stars', 'name'], default='stars', help="Sort results by (default: stars)")

    args = parser.parse_args()  # Parse command line arguments
    main(args)  # Execute main function
