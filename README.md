# BOM Data Scrapper
Python script that scrapes weather and climate data from the [Australian Bureau of Meterology](http://www.bom.gov.au/). 

While an effort has been made to make this platform independent, it has been designed and tested on Windows exclusively. 

## Requirements 
All Python environment requirements are listed in the `conda-env.yml` file for Conda use, a `requirements.txt` file is also included for Pip. 

The script requires the use of [Selenium WebDriver](https://www.selenium.dev/documentation/webdriver/), specifically the Chrome WebDriver. The script will look for the Chrome executable and the webdriver within the respective subdirectories in the `selenium` folder. 

## Note 
Testing files incomplete