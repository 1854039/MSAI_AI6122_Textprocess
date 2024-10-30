import argparse
from datetime import datetime
import nltk
from whoosh.qparser import QueryParser
from create_index import preprocess_text

nltk.download('stopwords')
# 定义索引结构
import os
import time
import pandas as pd
import matplotlib.pyplot as plt
from whoosh.index import create_in, open_dir

BUSINESS_INDEX_DIR = "indexdir"
REVIEW_INDEX_DIR = "review_indexdir"

# 打开索引
ix = open_dir(BUSINESS_INDEX_DIR)
review_ix = open_dir(REVIEW_INDEX_DIR)

def search_business(query_str, top_n, sort_by='stars'):
    """
    根据关键词搜索商家。

    参数:
        query_str (str): 搜索关键词。
        top_n (int): 返回的结果数量。
        sort_by (str): 排序方式，可以是'stars'或'name'。

    返回:
        list: 包含搜索结果的列表。
    """
    query_str = preprocess_text(query_str)
    start_time = time.time()  # 开始计时
    with ix.searcher() as searcher:
        query = QueryParser("name", ix.schema).parse(query_str)

        results = searcher.search(query,limit=None,sortedby=sort_by)
        # 收集结果
        businesses = [(result['business_id'], result['name'], result['stars']) for result in results]

        # 根据指定的排序方式排序
        if sort_by == 'stars':
            businesses.sort(key=lambda x: x[2], reverse=True)  # 按星级降序
        elif sort_by == 'name':
            businesses.sort(key=lambda x: x[1])  # 按名称升序

    elapsed_time = time.time() - start_time  # 结束计时
    total_results = len(businesses)  # 获取总结果数量
    print(f"Search Businesses executed in {elapsed_time:.4f} seconds. Found {total_results} results.")

    # 返回前N个结果
    if top_n > total_results:
        print(f"Requested top_n ({top_n}) exceeds available results. Returning all {total_results} results.")
        return businesses  # 如果请求的数量超过可用结果，返回所有结果
    else:
        return businesses[:top_n]  # 返回前N个结果


def search_reviews(query_str, top_n=10, sort_by='useful'):
    """
    根据关键词搜索评论。

    参数:
        query_str (str): 搜索关键词。
        top_n (int): 返回的结果数量，默认为10。
        sort_by (str): 排序方式，可以是'useful'或'time'。

    返回:
        list: 包含搜索结果的列表。
    """
    query_str = preprocess_text(query_str)

    start_time = time.time()  # 开始计时
    with review_ix.searcher() as searcher:
        query = QueryParser("text", review_ix.schema).parse(query_str)
        print(query)
        results = searcher.search(query, limit=None,sortedby=sort_by)
        print(results)

        # 收集结果
        reviews = [
            (result['review_id'], result['text'], result['stars'], result['useful'], result['date'])
            for result in results
        ]

        # 根据排序方式进行排序
        if sort_by == 'date':
            reviews.sort(key=lambda x: datetime.strptime(x[4], "%Y-%m-%d %H:%M:%S"), reverse=True)  # 按时间降序
        elif sort_by == 'useful':
            reviews.sort(key=lambda x: x[3], reverse=True)  # 按'有用'数降序

    elapsed_time = time.time() - start_time  # 结束计时
    print(f"Search Reviews executed in {elapsed_time:.4f} seconds.")
    return reviews[:top_n]  # 返回前N个结果


def search_geospatial(lat_min, lat_max, lon_min, lon_max, top_n=10, sort_by='stars'):
    """
    执行地理空间搜索。

    返回:
        list: 包含搜索结果的列表。
    """
    start_time = time.time()  # 开始计时
    results = []
    epsilon = 1e-7  # 设定容忍度
    with ix.searcher() as searcher:
        for hit in searcher.documents():
            lat = float(hit['latitude'])
            lon = float(hit['longitude'])

            if (lat_min - epsilon <= lat <= lat_max + epsilon) and (
                    lon_min - epsilon <= lon <= lon_max + epsilon):
                results.append((hit['business_id'], hit['name'], lat, lon, hit['stars']))

    # 根据指定的排序方式排序
    if sort_by == 'stars':
        results.sort(key=lambda x: x[4], reverse=True)  # 按星级降序
    elif sort_by == 'name':
        results.sort(key=lambda x: x[1])  # 按名称升序

    elapsed_time = time.time() - start_time  # 结束计时
    print(f"Search Geospatial executed in {elapsed_time:.4f} seconds.")

    # 可视化查询结果
    plt.figure(figsize=(10, 6))

    # 绘制所有结果的经纬度
    all_lats = [result[2] for result in results]
    all_lons = [result[3] for result in results]

    # 用蓝色点标注所有结果
    plt.scatter(all_lons, all_lats, color='blue', label='All Results', alpha=0.5)

    # 只绘制前 N 个结果，用红色点标注
    if results:
        top_lats = [result[2] for result in results[:top_n]]
        top_lons = [result[3] for result in results[:top_n]]
        plt.scatter(top_lons, top_lats, color='red', label='Top N Results', alpha=0.7)

    # 绘制查询范围的方框
    plt.gca().add_patch(plt.Rectangle((lon_min, lat_min), lon_max - lon_min, lat_max - lat_min,
                                        fill=None, edgecolor='blue', linewidth=2, label='Search Area'))

    # 设置图形属性
    plt.xlim(-113.7313182, -113.2574946)
    plt.ylim(53.3526529, 53.6791969)
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title('Geospatial Search Results')
    plt.legend()
    plt.grid()
    plt.show()

    return results[:top_n]  # 返回前N个结果

def main(args):
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

    args = parser.parse_args()
    main(args)
