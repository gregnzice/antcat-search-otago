import pandas as pd
import requests
import json
import re


def gn_antcat_download(search_term, filter_terms):
    """
    Query AntCat GeoNetwork for records matching a controlled vocabulary
    search term and optional filter terms, then download and combine
    the associated datasets.

    Parameters:
        search_term  : str        - Primary keyword (controlled vocabulary)
        filter_terms : list[str]  - Additional keyword filters

    Returns:
        records      : pd.DataFrame - Metadata records (uuid, title, data_link)
        data         : pd.DataFrame - Combined downloaded datasets
    """

    BASE    = "https://antcat.antarcticanz.govt.nz/geonetwork"
    URL     = f"{BASE}/srv/api/search/records/_search?bucket=metadata&relatedType=datasets"
    HEADERS = {"accept": "application/json", "Content-Type": "application/json"}

    # ------------------------------------------------------------------
    # 1. Build request payload
    # ------------------------------------------------------------------
    def build_payload(search_term, filter_terms):
        filter_dict = {}

        payload = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "tag.default": {"value": search_term}
                            }
                        }
                    ],
                    "filter": {}
                }
            },
            "_source": {
                "includes": ["uuid", "linkUrl", "resourceTitleObject"]
            },
            "size": 100
        }

        for i, filter_term in enumerate(filter_terms):
            if filter_term != "":
                key = "term" if i == 0 else f"term.{i}"
                filter_dict[key] = {"tag.default": {"value": filter_term}}

        if filter_dict:
            payload["query"]["bool"]["filter"] = filter_dict

        return json.dumps(payload, indent=2)

    # ------------------------------------------------------------------
    # 2. Download and parse a single dataset (in-memory)
    # ------------------------------------------------------------------
    def fetch_dataset(metadata_url, data_title, data_link):
        if not data_link:
            print(f"  [SKIP] No data link for: {data_title}")
            return {"data": None, "problems": "No data link provided", "id": metadata_url}

        try:
            print(f"  [DOWNLOAD] {data_title}")
            response = requests.get(data_link)
            response.raise_for_status()

            # Decode content and find where data starts (after */ header)
            lines = response.content.decode("utf-8").splitlines()
            pattern = r"\*/"
            position = next((i for i, line in enumerate(lines) if re.search(pattern, line)), None)
            position = (position + 1) if position is not None else 0

            from io import StringIO
            content = "\n".join(lines[position:])
            df = pd.read_csv(StringIO(content), sep="\t")
            df["metadata_url"] = metadata_url

            print(f"  [OK] {len(df)} rows")
            return {"data": df, "problems": None, "id": metadata_url}

        except Exception as e:
            print(f"  [ERROR] {data_title}: {e}")
            return {"data": None, "problems": e, "id": metadata_url}

    # ------------------------------------------------------------------
    # 3. Query the API
    # ------------------------------------------------------------------
    print(f"Querying AntCat for: '{search_term}' with filters: {filter_terms}")
    payload = build_payload(search_term, filter_terms)

    resp = requests.post(URL, headers=HEADERS, data=payload)
    resp.raise_for_status()
    print(f"Response status: {resp.status_code}")

    parsed   = json.loads(resp.text)
    hits     = parsed["hits"]["hits"]
    print(f"Total records found: {len(hits)}")

    # ------------------------------------------------------------------
    # 4. Build records DataFrame
    # ------------------------------------------------------------------
    extracted = []
    for item in hits:
        title = item["_source"]["resourceTitleObject"]["default"]
        extracted.append({
            "metadata_url": f"{BASE}/srv/eng/catalog.search#/metadata/{item['_id']}",
            "data_title":   re.sub(r"[^a-zA-Z0-9-]", " ", title),
            "data_link":    item["_source"].get("linkUrl")
        })

    records = pd.DataFrame(extracted)
    print(f"\nRecords DataFrame ({len(records)} rows):")
    print(records)

    # ------------------------------------------------------------------
    # 5. Download all datasets
    # ------------------------------------------------------------------
    print(f"\nDownloading {len(records)} datasets...")
    results = records.apply(
        lambda row: fetch_dataset(
            metadata_url=row["metadata_url"],
            data_title=row["data_title"],
            data_link=row["data_link"]
        ),
        axis=1
    )

    # ------------------------------------------------------------------
    # 6. Combine into single DataFrame
    # ------------------------------------------------------------------
    frames = [r["data"] for r in results if r["data"] is not None]

    if frames:
        data = pd.concat(frames, ignore_index=True)
        print(f"\nCombined data DataFrame ({len(data)} rows, {len(data.columns)} columns):")
        print(data.head())
    else:
        data = pd.DataFrame()
        print("\nNo data successfully downloaded.")

    return records, data