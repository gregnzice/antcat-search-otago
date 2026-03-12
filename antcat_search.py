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
    fetch_points=True,
    fetch_author=True
):
    """
    Query AntCat GeoNetwork using:
        - Bounding box
        - Temporal extent
        - Optional free-text search term

    Automatically paginates internally and returns ALL results (<10k).

    Parameters:
        bbox             : (min_lon, min_lat, max_lon, max_lat)
        date_from        : str  - ISO date string e.g. "2015-01-01"
        date_to          : str  - ISO date string e.g. "2020-12-31"
        search_term      : str  - Optional free-text search term
        page_size        : int  - Number of results per page (default 500)
        spatial_relation : str  - Spatial relation (default "intersects")
        time_relation    : str  - Temporal relation (default "intersects")
        fetch_points     : bool - Fetch point geometries from XML (default True)
        fetch_author     : bool - Fetch author name from XML (default True)

    Returns:
        title
        abstract
        start_date
        end_date
        bounding_box
        coordinate     (list of (lat, lon) tuples, only if fetch_points=True)
        author         (str, only if fetch_author=True)
        record_url
    """

    BASE = "https://antcat.antarcticanz.govt.nz/geonetwork"
    URL = f"{BASE}/srv/api/search/records/_search"
    PARAMS = {"bucket": "metadata", "relatedType": "datasets"}
    HEADERS = {"accept": "application/json",
               "Content-Type": "application/json"}

    # ------------------------------------------------------------------
    # Helper: build envelope from bbox
    # ------------------------------------------------------------------
    def envelope_from_bbox(min_lon, min_lat, max_lon, max_lat):
        lo = min(min_lon, max_lon)
        hi = max(min_lon, max_lon)
        south = min(min_lat, max_lat)
        north = max(min_lat, max_lat)
        return [[lo, north], [hi, south]]  # [[minLon,maxLat],[maxLon,minLat]]

    # ------------------------------------------------------------------
    # Helper: parse temporal extent
    # ------------------------------------------------------------------
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
            end = record.get("lte")
            if start and not end:
                end = start
            return start, end
        except:
            return None, None

    # ------------------------------------------------------------------
    # Helper: build text search clause
    # ------------------------------------------------------------------
    def build_text_clause(term):
        terms = term.strip().split()
        if len(terms) > 1:
            return f'+anytext:"{term}"'  # phrase match
        else:
            return f'+anytext:{term}'    # single token match

    # ------------------------------------------------------------------
    # Helper: fetch XML record and parse points and/or author
    # ------------------------------------------------------------------
    def fetch_xml_fields(uuid):
        """
        Fetches the full XML record once and extracts:
            - point geometries (if fetch_points=True)
            - author name      (if fetch_author=True)
        Returns a dict with keys 'points' and/or 'author'.
        """
        url = f"{BASE}/srv/api/records/{uuid}/formatters/xml"
        result = {}

        try:
            resp = requests.get(url, headers={"accept": "application/xml"})
            resp.raise_for_status()
            xml = resp.text

            if fetch_points:
                matches = re.findall(
                    r'<gml:pos[^>]*>([\-\d\.\s]+)</gml:pos>', xml)
                points = []
                for m in matches:
                    parts = m.strip().split()
                    if len(parts) == 2:
                        lat, lon = float(parts[0]), float(parts[1])
                        points.append((lat, lon))
                result["coordinate"] = points

            if fetch_author:
                match = re.search(
                    r'<cit:CI_RoleCode[^>]*codeListValue="author".*?'
                    r'<cit:CI_Individual>.*?<cit:name>\s*<gco:CharacterString>(.*?)</gco:CharacterString>',
                    xml,
                    re.DOTALL
                )
                result["author"] = match.group(1).strip() if match else None

        except:
            if fetch_points:
                result["coordinate"] = []
            if fetch_author:
                result["author"] = None

        return result

    # ------------------------------------------------------------------
    # Helper: build Elasticsearch payload
    # ------------------------------------------------------------------
    env = envelope_from_bbox(*bbox)

    def build_payload(offset):
        bool_query = {
            "filter": [
                {"term": {"isTemplate": {"value": "n"}}},
                {
                    "geo_shape": {
                        "geom": {
                            "shape": {
                                "type":        "envelope",
                                "coordinates": env
                            },
                            "relation": spatial_relation
                        }
                    }
                },
                {
                    "range": {
                        "resourceTemporalExtentDateRange": {
                            "gte":      date_from,
                            "lte":      date_to,
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

    # ------------------------------------------------------------------
    # Main loop: paginate and build rows
    # ------------------------------------------------------------------
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
            abs_obj = src.get("resourceAbstractObject") or {}
            uuid = src.get("uuid")
            temporal = src.get("resourceTemporalExtentDateRange")

            start, end = parse_temporal_extent(temporal)

            row = {
                "title":        title_obj.get("default") or title_obj.get("langeng"),
                "abstract":     abs_obj.get("default") or abs_obj.get("langeng"),
                "start_date":   start,
                "end_date":     end,
                "bounding_box": src.get("geom"),
                "record_url":   f"{BASE}/srv/eng/catalog.search#/metadata/{uuid}"
            }

            # Single XML fetch if either flag is enabled
            if fetch_points or fetch_author:
                xml_fields = fetch_xml_fields(uuid)
                row.update(xml_fields)

            all_rows.append(row)

        offset += len(hits)

        if offset >= total or len(hits) == 0:
            break

    # ------------------------------------------------------------------
    # Build and clean DataFrame
    # ------------------------------------------------------------------
    df = pd.DataFrame(all_rows)

    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["end_date"] = pd.to_datetime(df["end_date"],   errors="coerce")

    if fetch_points:
        df["coordinate"] = df["coordinate"].apply(
            lambda x: pd.NA if isinstance(x, list) and len(x) == 0 else x
        )

    cols = [c for c in df.columns if c != "record_url"] + ["record_url"]
    df = df[cols]

    return df
