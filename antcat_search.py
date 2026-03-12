import requests
import pandas as pd

import requests
import pandas as pd
import re

def gn_antcat_search(
    bbox,
    date_from,
    date_to,
    search_term=None,
    *,
    page_size=500,
    spatial_relation="intersects",
    time_relation="intersects",
    fetch_points=False
):
    """
    Query AntCat GeoNetwork using:
        - Bounding box
        - Temporal extent
        - Optional free-text search term

    Automatically paginates internally and returns ALL results (<10k).

    Returns:
        uuid
        title
        abstract
        start_date
        end_date
        geom
        points         (list of (lat, lon) tuples, only if fetch_points=True)
        record_url
    """

    BASE = "https://antcat.antarcticanz.govt.nz/geonetwork"
    URL = f"{BASE}/srv/api/search/records/_search"
    PARAMS = {"bucket": "metadata", "relatedType": "datasets"}
    HEADERS = {"accept": "application/json", "Content-Type": "application/json"}

    def envelope_from_bbox(min_lon, min_lat, max_lon, max_lat):
        lo = min(min_lon, max_lon)
        hi = max(min_lon, max_lon)
        south = min(min_lat, max_lat)
        north = max(min_lat, max_lat)
        return [[lo, north], [hi, south]]  # [[minLon,maxLat],[maxLon,minLat]]

    def parse_temporal_extent(ext):
        """
        Handles:
            [{'gte': 'start', 'lte': 'end'}]
            [{'gte': 'start'}]

        Mirrors end_date = start_date if missing.
        """
        if not ext:
            return None, None

        try:
            record = ext[0]
            start = record.get("gte")
            end   = record.get("lte")

            if start and not end:
                end = start

            return start, end
        except:
            return None, None

    def build_text_clause(term):
        terms = term.strip().split()
        if len(terms) > 1:
            return f'+anytext:"{term}"'   # phrase match
        else:
            return f'+anytext:{term}'     # single token match

    def parse_point_geometries(uuid):
        """
        Fetch full XML record and extract all gml:Point coordinates.
        Returns list of (lat, lon) tuples.
        Note: gml:pos is ordered lat lon (not lon lat).
        """
        url = f"{BASE}/srv/api/records/{uuid}/formatters/xml"
        try:
            resp = requests.get(url, headers={"accept": "application/xml"})
            resp.raise_for_status()
            matches = re.findall(r'<gml:pos[^>]*>([\-\d\.\s]+)</gml:pos>', resp.text)
            points = []
            for m in matches:
                parts = m.strip().split()
                if len(parts) == 2:
                    lat, lon = float(parts[0]), float(parts[1])
                    points.append((lat, lon))
            return points
        except:
            return []

    env = envelope_from_bbox(*bbox)

    def build_payload(offset):
        bool_query = {
            "filter": [
                {"term": {"isTemplate": {"value": "n"}}},
                {
                    "geo_shape": {
                        "geom": {
                            "shape": {
                                "type": "envelope",
                                "coordinates": env
                            },
                            "relation": spatial_relation
                        }
                    }
                },
                {
                    "range": {
                        "resourceTemporalExtentDateRange": {
                            "gte": date_from,
                            "lte": date_to,
                            "relation": time_relation
                        }
                    }
                }
            ]
        }

        if search_term:
            bool_query["must"] = [
                {"query_string": {"query": build_text_clause(search_term)}}
            ]

        return {
            "from": offset,
            "size": page_size,
            "_source": {
                "includes": [
                    "uuid",
                    "resourceTitleObject*",
                    "resourceAbstractObject*",
                    "resourceTemporalExtentDateRange",
                    "geom"
                ]
            },
            "query": {"bool": bool_query}
        }

    all_rows = []
    offset = 0
    printed_total = False

    while True:
        payload = build_payload(offset)
        resp = requests.post(URL, params=PARAMS, headers=HEADERS, json=payload)
        resp.raise_for_status()
        data = resp.json()

        hits = data.get("hits", {}).get("hits", [])
        total = data["hits"]["total"]["value"]

        if not printed_total:
            print(f"Total hits: {total}")
            printed_total = True

        for h in hits:
            src = h.get("_source", {})
            title_obj = src.get("resourceTitleObject") or {}
            abs_obj   = src.get("resourceAbstractObject") or {}

            uuid     = src.get("uuid")
            temporal = src.get("resourceTemporalExtentDateRange")

            start, end = parse_temporal_extent(temporal)

            row = {
                "title":      title_obj.get("default") or title_obj.get("langeng"),
                "abstract":   abs_obj.get("default")   or abs_obj.get("langeng"),
                "start_date": start,
                "end_date":   end,
                "bounding_box":       src.get("geom"),
                "record_url": f"{BASE}/srv/eng/catalog.search#/metadata/{uuid}"
            }

            if fetch_points:
                row["coordinate"] = parse_point_geometries(uuid)

            all_rows.append(row)

        offset += len(hits)

        if offset >= total or len(hits) == 0:
            break

    df = pd.DataFrame(all_rows)

    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["end_date"]   = pd.to_datetime(df["end_date"],   errors="coerce")

    if fetch_points:
        df["coordinate"] = df["coordinate"].apply(lambda x: pd.NA if isinstance(x, list) and len(x) == 0 else x)

    cols = [c for c in df.columns if c != "record_url"] + ["record_url"]
    df = df[cols]
    
    return df