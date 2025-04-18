""" Module for scraping BoM data """

import re
import os
from typing import Dict, Any, List, Tuple

import pyproj
import shapely
import requests
import numpy as np
import pandas as pd
import geopandas as gpd

from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException 

tqdm.pandas()

class Scrapper:
  def __init__(self, proj: int = 7899):
    self.crs = pyproj.crs.CRS(proj)

  def get(
    self, 
    url: str, 
    opts: Dict[str, str]|None = None, 
    header: Dict[str, Any]|None = None, 
    essential: bool = True
  ) -> requests.Response|None:
    """
    Make a get request, do any status code error handling, and return response if successful
    :param url: the request url
    :param opts: any parameters to add to the url via the params argument in requests.get()
    :param header: headers for the request
    :param essential: if the request is essential is to the continued processing of the 
      application (should error or warning be given on failure)
    :return: a successful status response or None (of fails)
    :raises: an HTTPError if the request fails 
    """
    r = requests.get(url, params=opts, headers=header, timeout=10.0)

    if r.status_code != 200:
      if essential:
        # If response is import, throw appropriate errors
        r.raise_for_status()
      else:
        # If response was not important, throwing warning and continue
        return None
    return r

  def get_stations(self) -> gpd.GeoDataFrame:
    """Returns a geodataframe containing all BoM weather stations"""
    url = "https://reg.bom.gov.au/climate/data/lists_by_element/stations.txt"
    r = self.get(url)
    assert r is not None

    r_lines = r.text.splitlines()
    station_list = []
    header_line: str
    headers: List[str]
    indicies: List[Tuple[int, int]]
    for line_num, line in enumerate(r_lines):
      if line_num < 2:
        continue
      if line_num==2:
        header_line = line
      elif line_num==3:
        indicies = [(m.start(0), m.end(0)) for m in re.finditer('-+', line)]
        headers = [header_line[start:end].strip() for start,end in indicies]
      else:
        station_list.append([line[start:end].strip() for start,end in indicies])

    station_list = station_list[0:len(station_list)-6]
    stations_df = pd.DataFrame(data=station_list, columns=headers).replace('', np.nan)
    stations = gpd.GeoDataFrame(
      stations_df,
      geometry=gpd.points_from_xy(stations_df['Lon'], stations_df['Lat'], crs='epsg:4326'),
      crs='epsg:4326'
    )
    stations = stations.to_crs(crs=self.crs)
    return stations

  def get_data(self, stations:List[str]) -> pd.DataFrame:
    """
    Gets the monthly rainfall data for the specified station 
    :param station: the station id for the desired station 
    :return: a `pandas.DataFrame` with the data for the station, or None
    """
    selenium_path = os.path.join(os.getcwd(), 'selenium')

    options = webdriver.ChromeOptions()
    options.binary_location = os.path.join(selenium_path, 'chrome', 'chrome.exe')
    options.add_argument("--unsafely-treat-insecure-origin-as-secure=http://www.bom.gov.au")
    # options.add_argument('--headless')
    # options.add_argument('--no-sandbox')

    service = webdriver.ChromeService(executable_path=os.path.join(selenium_path, 'chromedriver', 'chromedriver.exe'))
    driver = webdriver.Chrome(service=service, options=options)

    rainfall_data = np.empty((0,3))
    stations_used = np.full(len(stations), True, dtype=bool)

    for index, station in enumerate(tqdm(stations)): 
      url = f'http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_stn_num={station}&p_nccObsCode=139&p_display_type=dataFile'
      driver.get(url)

      try:
        data_table = driver.find_element(By.ID, 'dataTable')
      except NoSuchElementException:
        stations_used[index] = False
        continue
      rows = data_table.find_elements(By.TAG_NAME, 'tr')

      for row in rows:
        if row.get_attribute('class') != '':
          continue 
        
        try: 
          year = row.find_element(By.TAG_NAME, 'th').text 
          cells = row.find_elements(By.TAG_NAME, 'td')

          rainfall = cells[-1].text 
          rainfall = np.nan if rainfall=='' else float(rainfall)
          rainfall_data = np.append(rainfall_data, np.array([station, year, rainfall]).reshape(1,3), axis=0)
        except NoSuchElementException:
          continue
        except IndexError:
          # Occurs in thead row 
          continue
    
    driver.quit()
    return pd.DataFrame(rainfall_data)

  def get_extent(self, locations: str|List[str], buffer: int = 0) -> gpd.GeoDataFrame:
    """
    Retrieves the geographic extent of the specified locations and applies a buffer.
    
    :param locations: A single location or a list of locations (state abbreviations).
    :param buffer: The buffer distance to apply to the extent geometry.
    :return: A GeoDataFrame containing the extent geometry.
    """
    ids = {"act": 8, "nsw": 1, "nt": 7, "qld": 3, "sa": 4, "tas": 6, "vic": 2, "wa": 5}
    opts = {"_profile": "oai", "_mediatype": "application/geo+json"}
    proj = pyproj.Transformer.from_crs(4326, self.crs, always_xy=True)

    locations = [locations] if isinstance(locations, str) else locations
    location_geoms: List[shapely.Polygon] | shapely.GeometryCollection = []
    for loc in locations:
      url = f"https://asgs.linked.fsdf.org.au/dataset/asgsed3/collections/STE/items/{ids[loc]}"
      r = self.get(url, opts)
      if r is None:
        continue

      geom = shapely.from_geojson(r.content)
      geom_convert_list = []
      for poly in geom.geoms:
        trans_coords = [proj.transform(x, y) for x,y in poly.exterior.coords]
        geom_convert_list.append(shapely.Polygon(trans_coords))

      geom_trans = shapely.MultiPolygon(geom_convert_list)
      geom_trans = shapely.buffer(geom_trans, buffer)

      location_geoms.append(geom_trans)

    # extent = shapely.GeometryCollection(location_geoms)
    extent = gpd.GeoDataFrame(geometry=location_geoms, crs=self.crs)
    return extent

  def run(self, state: str, buffer: int):
    """
    Executes the full workflow to retrieve weather station data within a state extent.

    :param state: The state abbreviation (e.g., 'tas', 'nsw').
    :param buffer: The buffer distance to apply to the state extent.
    :return: A GeoDataFrame containing weather station data.
    """
    extent = self.get_extent(state, buffer) 
    stations_all = self.get_stations()
    stations_extent = gpd.sjoin(stations_all, extent, predicate='within')
    rainfall = self.get_data(stations_extent['Site'].values)

    rainfall.to_csv('rainfall.csv', index=False)
    stations_extent.to_file('stations_extent.geojson', driver='GeoJSON')

if __name__=='__main__':
  scraper = Scrapper()
  scraper.run('tas', 0)
