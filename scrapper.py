# Modules 
import requests 
import pandas as pd
import geopandas as gpd
from shapely import Point
from bs4 import BeautifulSoup


def get_stations() -> gpd.GeoDataFrame: 
    """Returns a geodataframe of BOM weather stations"""
    URL = "https://reg.bom.gov.au/climate/data/lists_by_element/stations.txt"

    r = requests.get(URL) 
    if r.status_code!=200:
        # TODO Proper error handling 
        raise RuntimeError  

    r_lines = r.text.splitlines()
    station_list = []
    location_list = []
    for line_num, line in enumerate(r_lines):
        if line_num < 4 or line_num > len(r_lines)-7:
            # Skip file pre- and post-amble 
            continue 

        # TODO: Get a more future-proofed way of getting variables 
        station = {
            'id': line[0:7].strip(),
            'district': line[8:13].strip(),
            'name': line[14:54].strip(),
            'year_open': line[55:62].strip(),
            'year_close': line[63:70].strip() if line[66:71].strip()!=".." else pd.NA,
            'lat': line[71:79].strip(),
            'lon': line[80:89].strip(),
            'loc_source': line[90:104].strip() if line[90:104].strip()!="....." else pd.NA,
            'state': line[105:108].strip(),
            'height_m': line[109:119].strip() if line[109:119].strip()!=".." else pd.NA,
            'bar_ht': line[120:128].strip() if line[120:128].strip()!=".." else pd.NA
        }

        station_list.append([v for _,v in station.items()])
        location_list.append(Point(station['lat'], station['lon']))

    return gpd.GeoDataFrame(
        station_list, 
        columns=['id', 'district', 'name', 'year_open', 'year_close', 'lat', 'lon', 'loc_source', 'state', 'height_m', 'bar_ht'],
        geometry=location_list,
        crs='epsg:4326'
    ).to_crs('epsg:7844')