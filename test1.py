import time

from whoosh.fields import Schema, TEXT, ID,  NUMERIC, BOOLEAN
from whoosh.index import create_in, open_dir
import os
import pandas as pd
from whoosh.qparser import QueryParser
from collections import Counter
from sklearn.feature_extraction.text import CountVectorizer
from whoosh.analysis import StemmingAnalyzer
# 定义索引结构
schema = Schema(
    business_id=ID(stored=True),
    name=TEXT(stored=True),
    address=TEXT(stored=True),
    city=TEXT(stored=True),
    state=TEXT(stored=True),
    postal_code=TEXT(stored=True),
    latitude=NUMERIC(stored=True),
    longitude=NUMERIC(stored=True),
    stars=NUMERIC(stored=True),
    review_count=NUMERIC(stored=True),
    is_open=BOOLEAN(stored=True),
    attributes=TEXT(stored=True),
    categories=TEXT(stored=True),
    hours=TEXT(stored=True),
)

# 创建索引目录

# 创建索引目录和索引
def create_business_index():
    if not os.path.exists("indexdir"):
        os.mkdir("indexdir")
        ix = create_in("indexdir", schema)
        print("Index created successfully!")
        return ix
    else:
        # 检查索引文件是否存在
        if not os.path.exists("indexdir/segments"):
            ix = create_in("indexdir", schema)
            print("Index created successfully!")
            return ix
        else:
            ix = open_dir("indexdir")
            print("Index opened successfully!")
            return ix

# 创建索引
business_df = pd.read_csv('AB_business.csv')

# 创建索引实例
ix = create_business_index()

def index_businesses(business_df):
    """
    将商家数据索引到索引中。

    参数:
        business_df (DataFrame): 包含商家数据的数据帧。
    """
    writer = ix.writer()
    total_count = len(business_df)
    start_time = time.time()
    for index, row in business_df.iterrows():
        # Handle NaN values

        business_id = row["business_id"] if pd.notna(row["business_id"]) else ''
        name = row["name"] if pd.notna(row["name"]) else ''
        address = row["address"] if pd.notna(row["address"]) else ''
        city = row["city"] if pd.notna(row["city"]) else ''
        state = row["state"] if pd.notna(row["state"]) else ''
        postal_code = row["postal_code"] if pd.notna(row["postal_code"]) else ''
        latitude = float(row["latitude"]) if pd.notna(row["latitude"]) else 0.0
        longitude = float(row["longitude"]) if pd.notna(row["longitude"]) else 0.0
        stars = float(row["stars"]) if pd.notna(row["stars"]) else 0.0
        review_count = int(row["review_count"]) if pd.notna(row["review_count"]) else 0
        is_open = bool(row["is_open"]) if pd.notna(row["is_open"]) else False
        attributes = str(row["attributes"]) if pd.notna(row["attributes"]) else '{}'
        categories = row["categories"] if pd.notna(row["categories"]) else ''
        hours = str(row["hours"]) if pd.notna(row["hours"]) else '{}'

        writer.add_document(
            business_id=business_id,
            name=name,
            address=address,
            city=city,
            state=state,
            postal_code=postal_code,
            latitude=latitude,
            longitude=longitude,
            stars=stars,
            review_count=review_count,
            is_open=is_open,
            attributes=attributes,
            categories=categories,
            hours=hours,
        )
        if (index + 1) % (total_count // 10) == 0:  # 每10%记录一次
            elapsed_time = time.time() - start_time
            print(f"Indexed {index + 1} of {total_count} business documents. Time taken: {elapsed_time:.2f} seconds.")
    writer.commit()

# 如果索引文件不存在，调用 index_businesses
if not os.path.exists("indexdir/segments"):
    index_businesses(business_df)
else:
    print("Index already exists; no need to re-index.")

print("Businesses indexed successfully!")

review_schema = Schema(
    review_id=ID(stored=True),
    user_id=ID(stored=True),
    business_id=ID(stored=True),
    stars=NUMERIC(stored=True),
    useful=NUMERIC(stored=True),
    funny=NUMERIC(stored=True),
    cool=NUMERIC(stored=True),
    text=TEXT(stored=True),
    date=TEXT(stored=True),  # 处理日期
)

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
review_ix = create_review_index()
review_df = pd.read_csv('AB_review.csv')


def index_reviews(review_df):
    """
    将评论数据索引到索引中。

    参数:
        review_df (DataFrame): 包含评论数据的数据帧。
    """
    writer = review_ix.writer()
    total_count = len(review_df)
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
            text=row['text'],
            date=row['date']
        )
        # Output time taken for each 10%
        if (index + 1) % max(1, total_count // 10) == 0:
            elapsed_time = time.time() - start_time
            print(f"Indexed {index + 1} of {total_count} business documents. Time taken: {elapsed_time:.2f} seconds.")

    writer.commit()

# 检查索引文件是否存在
if not os.path.exists("review_indexdir/segments"):
    index_reviews(review_df)
else:
    print("Review index already exists. Skipping indexing.")

print("Reviews indexed successfully!")
def search_business(query_str):
    """
    根据关键词搜索商家。

    参数:
        query_str (str): 搜索关键词。

    返回:
        list: 包含搜索结果的列表。
    """
    with ix.searcher() as searcher:
        query = QueryParser("name", ix.schema).parse(query_str)
        results = searcher.search(query)
        return [(result['business_id'], result['name'], result['stars']) for result in results]

def search_reviews(query_str):
    """
    根据关键词搜索评论。

    参数:
        query_str (str): 搜索关键词。

    返回:
        list: 包含搜索结果的列表。
    """
    with review_ix.searcher() as searcher:
        query = QueryParser("text", review_ix.schema).parse(query_str)
        results = searcher.search(query)
        return [(result['review_id'], result['text'], result['stars']) for result in results]

def count_user_reviews(review_df):
    """
    统计用户评论数量。

    参数:
        review_df (DataFrame): 包含评论数据的数据帧。

    返回:
        Series: 用户评论数量统计。
    """
    return review_df['user_id'].value_counts()
def calculate_bounding_box(user_reviews):
    """
    计算用户评论的经纬度边界框。

    参数:
        user_reviews (DataFrame): 包含用户评论数据的数据帧。

    返回:
        tuple: 边界框坐标。
    """
    latitudes = user_reviews['latitude']
    longitudes = user_reviews['longitude']
    return (latitudes.min(), latitudes.max(), longitudes.min(), longitudes.max())
def top_n_words(reviews, n=10):
    """
    获取评论中最常见的n个词。

    参数:
        reviews (list): 评论文本列表。
        n (int): 最常见词的数量，默认为10。

    返回:
        list: 最常见词及其频率。
    """
    # Tokenize and remove stopwords
    vectorizer = CountVectorizer(stop_words='english')
    X = vectorizer.fit_transform(reviews)
    word_counts = X.sum(axis=0)
    words = vectorizer.get_feature_names_out()
    return Counter(dict(zip(words, word_counts.A1))).most_common(n)

def search_geospatial():
    """
    执行地理空间搜索。

    返回:
        list: 包含搜索结果的列表。
    """
    while True:
        try:
            lat_min = float(input("Enter minimum latitude: "))
            lat_max = float(input("Enter maximum latitude: "))
            lon_min = float(input("Enter minimum longitude: "))
            lon_max = float(input("Enter maximum longitude: "))

            if not (-90 <= lat_min <= 90 and -90 <= lat_max <= 90):
                print("Invalid latitude values. Latitude must be between -90 and 90.")
                continue
            if not (-180 <= lon_min <= 180 and -180 <= lon_max <= 180):
                print("Invalid longitude values. Longitude must be between -180 and 180.")
                continue
            if lat_min > lat_max or lon_min > lon_max:
                print("Minimum value must be less than or equal to maximum value.")
                continue

            results = []
            epsilon = 1e-7  # 设定容忍度
            with ix.searcher() as searcher:
                for hit in searcher.documents():
                    lat = float(hit['latitude'])
                    lon = float(hit['longitude'])

                    # 调试输出
                    print(f"Checking lat: {lat}, lon: {lon}")

                    if (lat_min - epsilon <= lat <= lat_max + epsilon) and (
                            lon_min - epsilon <= lon <= lon_max + epsilon):
                        results.append((hit['business_id'], hit['name'], lat, lon))

            return results

        except ValueError:
            print("Please enter valid numerical values.")


def main():
    print("Welcome to the Business Review Search Engine!")
    while True:
        print("\nOptions:")
        print("1. Search Businesses")
        print("2. Search Reviews")
        print("3. Geospatial Search")
        print("4. Exit")
        choice = input("Enter your choice: ")

        if choice == '1':
            query = input("Enter keywords to search for businesses: ")
            results = search_business(query)
            for res in results:
                print(f"ID: {res[0]}, Name: {res[1]}, Score: {res[2]}")

        elif choice == '2':
            query = input("Enter keywords to search for reviews: ")
            results = search_reviews(query)
            for res in results:
                print(f"Review ID: {res[0]}, Text: {res[1]}, Score: {res[2]}")

        elif choice == '3':

            results = search_geospatial()
            for res in results:
                print(f"ID: {res[0]}, Name: {res[1]}, Latitude: {res[2]}, Longitude: {res[3]}")

        elif choice == '4':
            break
        else:
            print("Invalid choice. Please try again.")
if __name__ == "__main__":
    # 启动程序
    main()
