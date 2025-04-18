import pytest
import requests
from unittest.mock import patch, MagicMock
import shapely
import pandas as pd
import geopandas as gpd

# Import context to set up the correct sys.path
import context
from bom_scrapper import Scrapper

@pytest.fixture
def scrapper():
    return Scrapper()

@patch("bomscrapper.scrapper.requests.get")
def test_get_success(mock_get, scrapper):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value =  {"id": 1, "title": "Test Post"}
    mock_get.return_value = mock_response

    response = scrapper.get('https://jsonplaceholder.typicode.com/posts/1')
    assert response == mock_response
    mock_get.assert_called_once_with('https://jsonplaceholder.typicode.com/posts/1', params=None, headers=None, timeout=10.0)

def test_get_failure_essential(scrapper):
    with pytest.raises(requests.exceptions.HTTPError):
        scrapper.get("https://jsonplaceholder.typicode.com/posts/9999", essential=True)

def test_get_failure_non_essential(scrapper):
    response = scrapper.get("https://jsonplaceholder.typicode.com/posts/9999", essential=False)
    assert response is None

@patch("bomscrapper.scrapper.requests.get")
def test_get_stations(mock_get, scrapper):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = (
        "Header Line 1\n"
        "Header Line 2\n"
        "Site   Name   Lat   Lon\n"
        "----   ----   ---   ---\n"
        "001    Test   -35.0 149.0\n"
        "002    Test2  -36.0 150.0\n"
        "Blah\n"
        "2 stations\n"
        "Blah\n"
        "(c) Copyright Commonwealth of Australia 2025, Bureau of Meteorology (ABN 92 637 533 532)\n"
        "Please note Copyright, Disclaimer and Privacy Notice, accessible at <http://www.bom.gov.au/other/copyright.shtml>\n"
        "Blah\n"
    )
    mock_get.return_value = mock_response

    stations = scrapper.get_stations()
    print(stations)
    assert isinstance(stations, gpd.GeoDataFrame)
    assert stations.shape == (2, 5)

@patch("bomscrapper.scrapper.requests.get")
def test_get_data(mock_get, scrapper):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b"Fake zip content"
    mock_get.return_value = mock_response

    with patch("zipfile.ZipFile") as mock_zip:
        mock_zip.return_value.__enter__.return_value.namelist.return_value = ["Data1.csv"]
        mock_zip.return_value.__enter__.return_value.open.return_value = MagicMock()
        mock_df = pd.DataFrame({"year": [2020], "month": [1], "rainfall": [100]})
        with patch("pandas.read_csv", return_value=mock_df):
            data = scrapper.get_data("001")
            assert isinstance(data, pd.DataFrame)
            assert len(data) == 1

@patch("bomscrapper.scrapper.requests.get")
def test_get_extent(mock_get, scrapper):
  mock_response = MagicMock()
  mock_response.status_code = 200
  mock_response.content = b'{"type": "MultiPolygon", "coordinates": [[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]]}'
  mock_get.return_value = mock_response

  extent = scrapper.get_extent("tas")
  assert isinstance(extent, gpd.GeoDataFrame)
  assert len(extent) == 1
