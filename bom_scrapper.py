""" Module for scraping BoM data """

import re
import os
import tkinter as tk
import tkinter.font as tkfont
import tkinter.messagebox as tkmsgbox
from typing import Dict, Any, List, Tuple

import pyproj
import shapely
import requests
import darkdetect
import numpy as np
import pandas as pd
from tqdm import tqdm
import geopandas as gpd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException 
from catppuccin import PALETTE as cat_palette

tqdm.pandas()

class Scrapper:
  def __init__(self, proj: str = '7899'):
    self.crs = pyproj.CRS(('epsg', proj))

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
    # options.add_argument('--headless=new')
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

  def run(self, state: str|List[str], buffer: int):
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

    try:
      os.makedirs(os.path.join(os.getcwd(), 'output_data'))
    except FileExistsError:
      pass 

    rainfall.to_csv(os.path.join('output_data', 'rainfall.csv'), index=False)
    stations_extent.to_file(os.path.join('output_data', 'stations'))

class GUI:
  def __init__(self):
    self.root = tk.Tk()

    # GUI Config 
    flavour = cat_palette.macchiato if darkdetect.isDark() else cat_palette.latte
    self.palette = {colour.identifier: colour.hex for colour in flavour.colors}

    font_fam = 'NotoSans NF' if 'NotoSans NF' in tkfont.families() else 'Arial'
    self.fonts = {
      'title': tkfont.Font(family=font_fam, size=18, weight='bold'),
      'head': tkfont.Font(family=font_fam, size=12, weight='bold'),
      'body': tkfont.Font(family=font_fam, size=12)
    }

    self.states = {
      "Australian Capital Territory": "nsw",
      "New South Wales": "nsw",
      "Northern Territory" : "nt",
      "Queensland": "qld",
      "South Australia": "sa",
      "Tasmania": "tas", 
      "Victoria": "vic",
      "Western Australia": "wa"
    }

    # GUI init
    self.root.title("BoM Rainfall Scrapper")
    self.root.geometry("500x500")
    self.root.configure(
      bg=self.palette['base']
    )

    self._state_select().pack(fill='x', padx=20, pady=10)
    self._option_row().pack(fill='x', padx=20, pady=10)
    # self._output_row().pack(fill='x', padx=20, pady=10)

    run_btn = tk.Button(self.root, text="Run", command=self.run, bg=self.palette['mantle'], font=self.fonts['head'], fg=self.palette['text'], activebackground=self.palette['green'], activeforeground=self.palette['base'])
    run_btn.pack(fill='x', padx=20, pady=10)

    # Start
    self.root.mainloop()

  def _state_select(self):
    frame = tk.Frame(self.root, bg=self.palette['base'], bd=0)
    frame.columnconfigure(0, weight=1)
    frame.columnconfigure(1, weight=2)

    state_names = tk.StringVar(value=[name for name in self.states.keys()])
    self.state_list = tk.Listbox(frame, listvariable=state_names, selectmode='multiple', bg=self.palette['mantle'], fg=self.palette['text'], highlightcolor=self.palette['blue'], width=30, bd=0, font=self.fonts['body'])
    self.state_list.grid(row=0, column=1)

    info_frame = tk.Frame(frame, bg=self.palette['base'], bd=0)
    info_frame.columnconfigure(0, weight=1)
    tk.Label(info_frame, text='Select states', font=self.fonts['head'], bg=self.palette['base'], fg=self.palette['text']).grid(row=0, column=0, sticky='w')
    tk.Label(info_frame, text='Can select multiple states', font=self.fonts['body'], bg=self.palette['base'], fg=self.palette['text']).grid(row=1, column=0, sticky='w')
    info_frame.grid(row=0, column=0, sticky='wn')

    return frame

  def _option_row(self): 
    frame = tk.Frame(self.root, bg=self.palette['base'], bd=0)
    frame.columnconfigure(0, weight=1, uniform='optRow')
    frame.columnconfigure(1, weight=1, uniform='optRow')

    buff_frame = tk.Frame(frame, bg=self.palette['base'], bd=0)
    self.buffer_distance = tk.DoubleVar(value=0)

    tk.Label(buff_frame, text='Buffer', font=self.fonts['head'], bg=self.palette['base'], fg=self.palette['text'], justify='left', wraplength=250).grid(row=0, column=0, sticky='w')
    tk.Label(buff_frame, text='The buffer distance (in km) to place around each state', font=self.fonts['body'], bg=self.palette['base'], fg=self.palette['text'], justify='left', wraplength=200).grid(row=1, column=0, sticky='w')
    tk.Scale(buff_frame, from_=0, to=500, variable=self.buffer_distance, orient='horizontal', bg=self.palette['base'], fg=self.palette['text'], showvalue=True, troughcolor=self.palette['mantle'], font=self.fonts['body'], borderwidth=0, highlightthickness=0, resolution=25, length=200).grid(row=2, column=0, sticky='w')

    buff_frame.grid(row=0, column=0, sticky='w')

    proj_frame = tk.Frame(frame, bg=self.palette['base'], bd=0)
    self.projection = tk.StringVar(value='3857')
    tk.Label(proj_frame, text='Projection', font=self.fonts['head'], bg=self.palette['base'], fg=self.palette['text'], justify='left', wraplength=250).grid(row=0, column=0, sticky='w')
    tk.Label(proj_frame, text='The EPSG code of the desired projection, default is WGS84 / Pseudo-Mercator', font=self.fonts['body'], bg=self.palette['base'], fg=self.palette['text'], justify='left', wraplength=250).grid(row=1, column=0, sticky='w')
    tk.Entry(proj_frame, text=self.projection, textvariable=self.projection, bg=self.palette['mantle'], fg=self.palette['text'], font=self.fonts['body'], bd=0).grid(row=2, column=0, sticky='w')
    proj_frame.grid(row=0, column=1)

    return frame
  
  def _output_row(self): 
    frame = tk.Frame(self.root, bg=self.palette['base'], bd=0)
    frame.columnconfigure(0, weight=1, uniform='optRow')
    frame.columnconfigure(1, weight=1, uniform='optRow')

    buff_frame = tk.Frame(frame, bg=self.palette['base'], bd=0)
    self.buffer_distance = tk.DoubleVar(value=0)

    tk.Label(buff_frame, text='Buffer', font=self.fonts['head'], bg=self.palette['base'], fg=self.palette['text'], justify='left', wraplength=250).grid(row=0, column=0, sticky='w')
    tk.Label(buff_frame, text='The buffer distance (in km) to place around each state', font=self.fonts['body'], bg=self.palette['base'], fg=self.palette['text'], justify='left', wraplength=200).grid(row=1, column=0, sticky='w')
    tk.Scale(buff_frame, from_=0, to=500, variable=self.buffer_distance, orient='horizontal', bg=self.palette['base'], fg=self.palette['text'], showvalue=True, troughcolor=self.palette['mantle'], font=self.fonts['body'], borderwidth=0, highlightthickness=0, resolution=25, length=200).grid(row=2, column=0, sticky='w')

    buff_frame.grid(row=0, column=0, sticky='w')

    proj_frame = tk.Frame(frame, bg=self.palette['base'], bd=0)
    self.projection = tk.StringVar(value='3857')
    tk.Label(proj_frame, text='Projection', font=self.fonts['head'], bg=self.palette['base'], fg=self.palette['text'], justify='left', wraplength=250).grid(row=0, column=0, sticky='w')
    tk.Label(proj_frame, text='The EPSG code of the desired projection, default is WGS84 / Pseudo-Mercator', font=self.fonts['body'], bg=self.palette['base'], fg=self.palette['text'], justify='left', wraplength=250).grid(row=1, column=0, sticky='w')
    tk.Entry(proj_frame, text=self.projection, textvariable=self.projection, bg=self.palette['mantle'], fg=self.palette['text'], font=self.fonts['body'], bd=0).grid(row=2, column=0, sticky='w')
    proj_frame.grid(row=0, column=1)

    return frame

  def popup_done(self):
    win = tk.Toplevel()
    win.wm_title("Complete!")
    win.configure(bg=self.palette['base'])

    frame = tk.Frame(win, bg=self.palette['base'], bd=0)
    tk.Label(frame, text="Process successfully complete! Files can be found at: ", bg=self.palette['base'], fg=self.palette['text'], font=self.fonts['body']).grid(row=0, column=0, sticky='w')
    tk.Label(frame, text=os.path.join(os.getcwd(), 'output_data'), bg=self.palette['base'], fg=self.palette['text'], font=self.fonts['body']).grid(row=1, column=0, sticky='w')
    tk.Button(frame, text="Okay", command=self.root.destroy, bg=self.palette['mantle'], font=self.fonts['head'], fg=self.palette['text'], activebackground=self.palette['red'], activeforeground=self.palette['base']).grid(row=2, column=0, sticky='e')
    frame.pack(padx=10, pady=10, fill='both')

  def run(self):
    states_used = [self.states[self.state_list.get(i)] for i in self.state_list.curselection()]
    if len(states_used)==0:
      tkmsgbox.showinfo("ERROR", "At least one state must be selected")
      return 
    
    buffer = int(self.buffer_distance.get())

    try:
      proj_code = self.projection.get()
      if proj_code == '':
        proj_code = '3857'
      scrapper = Scrapper(proj_code)
    except pyproj.exceptions.CRSError:
      tkmsgbox.showinfo("ERROR", f"Unrecognised projection: EPSG:{proj_code}")
      return 
    
    scrapper.run(states_used, buffer)
    self.popup_done()

if __name__=='__main__':
  gui = GUI()