# AntCat GeoNetwork Search Script

This repository provides a standalone Python script for performing programmatic spatial–temporal search of the AntCat GeoNetwork catalogue.

The script allows you to query catalogue records using:
- a spatial bounding box
- a temporal extent (dataset coverage)
- an optional free‑text search term

The function returns matching records as a Pandas DataFrame.

---

## Getting Started (Notebook Workflow)

Use the following in a Jupyter notebook to download the script from GitHub, import the search function, run a query, and view the results.

### 1. Download the Script
```python
import requests

url = "https://raw.githubusercontent.com/antarcticanz/antcat-search/main/antcat_search.py"
with open("antcat_search.py", "wb") as f:
    f.write(requests.get(url).content)
```

### 2. Import the Function
```python
from antcat_search import gn_antcat_search
```

### 3. Example Usage
```python
ds = gn_antcat_search(
    bbox=(158, -78.3, 175, -75.7),
    date_from="2015-01-01",
    date_to="2020-12-31",
    search_term="sea ice"
)
ds
```
