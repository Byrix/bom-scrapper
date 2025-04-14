#pylint: disable=C0326

import re
import io
import zipfile 
from typing import Dict, Any, List, Tuple

import pyproj
import shapely
import requests
import numpy as np
import pandas as pd
import geopandas as gpd
from numpy.typing import NDArray

class Scrapper:
    def __init__(self):
        pass

    def _get(
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

    def _convert_crs(self, coords: List[float]) -> NDArray[np.float64]:
        """
        Transforms a shapely geometry from web-mercator (EPSG:3877) to GDA2020 (EPSG:7844)
        :param points: an array of point coordinates [x, y, (z)]
        :return: a (n,2) shaped array of coordinate points, where the i-th entry is the coordinates 
            from points[i] converted to GDA2020 
        """
        # TODO: Potentially change to using a partial function in the method where this is required 
        # or convert this to a partial in some form
        # Reference: https://shapely.readthedocs.io/en/stable/manual.html#shapely.ops.transform
        proj = pyproj.Transformer.from_crs(3857, 7899)
        return np.vectorize(proj.transform)(*coords.T)

    def get_stations(self) -> gpd.GeoDataFrame:
        """Returns a geodataframe containing all BoM weather stations"""
        url = "https://reg.bom.gov.au/climate/data/lists_by_element/stations.txt"
        r = self._get(url)
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
        stations = stations.to_crs(epsg=7899)
        return stations

    def get_data(self, station:str) -> pd.DataFrame|None:
        """
        Gets the monthly rainfall data for the specified station 
        :param station: the station id for the desired station 
        :return: a `pandas.DataFrame` with the data for the station, or None
        """
        url = r"http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av"
        opts = {
            "p_stn_num": str(station),
            "p_nccObsCode": "139",
            "p_display_type": "monthlyZippedDataFile",
            "p_c": "-1487270503",
        }
        headers = {"User-Agent": "Mozilla/5.0"}  # Treat as browser not bot

        # Get data
        r = self._get(url, opts, headers)
        assert r is not None

        columns = ["product_code", "id", "year", "month", "rainfall", "quality"]
        try:
            with zipfile.ZipFile(io.BytesIO(r.content)) as zipf:
                df = pd.concat(
                    [
                        pd.read_csv(zipf.open(file), header=0, names=columns)
                        for file in zipf.namelist()
                        if file.endswith("Data1.csv")
                    ]
                )
        except zipfile.BadZipFile:
            return None

        return df

    def get_extent(self, locations: str|List[str], buffer: int = 0) -> gpd.GeoDataFrame:
        """

        """
        ids = {"act": 8, "nsw": 1, "nt": 7, "qld": 3, "sa": 4, "tas": 6, "vic": 2, "wa": 5}
        opts = {"_profile": "oai", "_mediatype": "application/geo+json"}
        proj = pyproj.Transformer.from_crs(3857, 7899)

        locations = [locations] if type(locations) is str else locations
        location_geoms: List[shapely.Polygon] | shapely.GeometryCollection = []
        for loc in locations:
            url = r"https://asgs.linked.fsdf.org.au/dataset/asgsed3/collections/STE/items/{feature_code}".format(
                feature_code=ids[loc]
            )
            r = self._get(url, opts)
            if r is None:
                continue

            geom = shapely.from_geojson(r.content)
            # geom = shapely.transform(geom, self._convert_crs)
            geom_convert_list = []
            for poly in geom.geoms:
                trans_coords = [proj.transform(x, y) for x,y in poly.exterior.coords]
                geom_convert_list.append(shapely.Polygon(trans_coords))

            geom_trans = shapely.MultiPolygon(geom_convert_list)
            # geom_trans = shapely.buffer(geom, buffer)

            location_geoms.append(geom_trans)

        # TODO: Some kind of dissolve? Maybe both before each extent is added as well as
        # before the full collection is returned. Feels more useful to be able to return
        # a Polygon or MultiPolygon rather than a GeomCollection
        extent = shapely.GeometryCollection(location_geoms)
        # Convert to polygon
        extent = gpd.GeoDataFrame(geometry=location_geoms, crs=7899)
        return extent

# NOTE: Probably overkill to have this function that's one single join operation 
    def filter_stations(
        self, 
        extent: gpd.GeoDataFrame, 
        stations: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """Returns a geodataframe of weather stations that lie within the extent"""
        extent_stations = gpd.sjoin(
            extent,
            stations,
            how="right",
            predicate='within'
        )
        return extent_stations

    def run(self, state:str, buffer:int) -> gpd.GeoDataFrame:
        extent = self.get_extent(state, buffer) 
        print("Extent done")
        stations_all = self.get_stations()
        print("Stations done")
        print(stations_all.head(2))
        print(stations_all.columns)

        stations_extent = self.filter_stations(extent, stations_all)
        print("Stations filtered")

        stations_extent.to_file('stations.geojson', driver='GeoJSON')
        print(stations_extent.columns)
        weather_list = []
        stations_extent.apply(lambda row: weather_list.append(self.get_data(row['Site'])), axis=1)
        print("Weather gotten")

        weather_list = np.asarray(weather_list)
        weather_list = weather_list[~np.isnan(weather_list)]

        # TODO: Convert from all entries to average annual 

        weather_df = gpd.GeoDataFrame( pd.concat(weather_list, ignore_index=True), crs=7844 )
        return weather_df


if __name__=='__main__':
    scraper = Scrapper()
    weather = scraper.run('tas', 0)
    weather.to_file('test.geojson', driver='GeoJSON')
