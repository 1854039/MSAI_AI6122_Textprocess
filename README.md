# Business Review Search Engine

## Overview

This project implements a search engine using the Whoosh library to index and search business and review data from Yelp dataset. Users can search for businesses and reviews based on keywords and perform geospatial searches based on geographic coordinates.

## Features

- **Business Search**: Search for businesses using keywords from their names.
- **Review Search**: Search for reviews based on specific keywords or phrases.
- **Geospatial Search**: Find businesses located within a specified geographical bounding box.
- **Indexing**: Index business and review data from CSV files.
- **Ranking Results**: Display search results with relevant information, including rankings and scores.

## Requirements

Ensure you have the following Python packages installed:

- Whoosh
- pandas
- scikit-learn

You can install the required packages using:

```bash
pip install -r requirements.txt
```
## Usage

Upon running the script, you will be presented with a menu of options:  
运行脚本后，您将看到一个选项菜单：

- **Search Businesses:** 搜索企业：
  - Input keywords to search for businesses by name.  
    输入关键字以按名称搜索企业。

- **Search Reviews:** 搜索评论：
  - Input keywords to find specific reviews.  
    输入关键字以查找特定评论。

- **Geospatial Search:** 地理空间搜索：
  - Enter the minimum and maximum latitude and longitude to find businesses within a specified area.  
    输入最小和最大纬度和经度以查找指定区域内的企业。

### Sample Commands 示例命令

- To search for businesses: 
  ```plaintext
  Enter keywords to search for businesses: pizza
- To search for reviews: 
  ```plaintext
  Enter keywords to search for reviews: great service
- To perform a geospatial search:
  ```plaintext
  Enter minimum latitude: 34.0
  Enter maximum latitude: 35.0
  Enter minimum longitude: -118.5
  Enter maximum longitude: -117.5

### Indexing 索引

- The indexing process will occur automatically when you run the script. The script will create directories for business and review indexes (`indexdir` and `review_indexdir`) if they do not exist.  
运行text1.py,索引过程将自动进行。如果索引不存在，将为business和review索引（`indexdir` 和 `review_indexdir`）创建目录并构建索引，否则读取已有的索引文件。
- The script commits the indexed data every 10% to ensure that progress is saved and can be monitored.该脚本每 10% 提交一次索引数据。

### Other function(TOBEUPDATE)
#### 1. `count_user_reviews(review_df)`
统计用户评论数量。

- **参数**:
  - `review_df (DataFrame)`: 包含评论数据的数据帧。
- **返回**:
  - `Series`: 用户评论数量统计。

#### 2. `calculate_bounding_box(user_reviews)`
计算用户评论的经纬度边界框。

- **参数**:
  - `user_reviews (DataFrame)`: 包含用户评论数据的数据帧。
- **返回**:
  - `tuple`: 边界框坐标，格式为 `(min_latitude, max_latitude, min_longitude, max_longitude)`。

#### 3. `top_n_words(reviews, n=10)`
获取评论中最常见的 n 个词。

- **参数**:
  - `reviews (list)`: 评论文本列表。
  - `n (int)`: 最常见词的数量，默认为 10。
- **返回**:
  - `list`: 最常见词及其频率的列表。

