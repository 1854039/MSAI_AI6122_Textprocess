# Search Engine
## Overview
This project includes two main Python scripts: `create_index.py` and `search.py`. These scripts handle building and querying an index to support efficient data retrieval from a specified dataset.

## File Descriptions

### `create_index.py`
- **Description**: This script is responsible for building an index by processing the input dataset and storing it in an index file for efficient querying.
- **Main Functions**:
  - Loads data from a specified file path.
  - Builds an efficient index structure for rapid searching.
  - Saves the generated index to disk for later retrieval.
- **Usage**:
  1. Ensure the data file path is correctly set.
  2. Run the script from the command line:
     ```bash
     python create_index.py --data_path path/to/data --index_path path/to/save/index
     ```
  - **Parameters**:
    - `--data_path`: Path to the data file.
    - `--index_path`: Path to save the generated index.

### `search.py`
- **Description**: This script allows for querying an existing index to return results that match the user's search query.
- **Main Functions**:
  - Loads a pre-built index.
  - Takes a user search query as input.
  - Searches the index for matching results and ranks them by relevance.
  - Outputs or saves the search results.
- **Usage**:
  1. Ensure the index file has been created.
  2. Run the script from the command line:
     ```bash
     python search.py --index_path path/to/index --query "your search query"
     ```
  - **Parameters**:
    - `--index_path`: Path to the index file.
    - `--query`: Search query.

## Environment Setup
- **Python Version**: Requires Python 3.7 or higher.
- **Dependencies**: 
  - List any necessary libraries here, such as `numpy`, `scipy`, etc.

Install the required Python packages by running:
```bash
pip install -r requirements.txt
```

## Quick Start
1. Run `create_index.py` to generate the index.
2. Run `search.py` to perform a search.

## Examples
### Building the Index
```bash
python create_index.py --data_path data/sample_data.txt --index_path index/sample_index.idx
```

### Performing a Search
```bash
python search.py --index_path index/sample_index.idx --query "example search term"
```



## License
This project is licensed under the MIT License.
