@echo off

set venv_name=bom_scrapper
set script_name=bom_scrapper.py

if not exist "%venv_name%" (
    echo Creating virtual environment in %venv_name%...
    python -m venv %venv_name%
    if errorlevel 1 (
        echo Failed to create virtual environment.
        exit /b 1
    )

    echo Installing requirements...
    call %venv_name%\Scripts\pip install -r requirements.txt
     if errorlevel 1 (
        echo Failed to install requirements.
        exit /b 1
    )
) else (
  echo Virtual environment detected, continuing... 
)

set "ps=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"

if not exist "%CD%\selenium\chromedriver" (
  echo Getting ChromeDriver

  set "url=https://storage.googleapis.com/chrome-for-testing-public/135.0.7049.114/win64/chromedriver-win64.zip"
  set "zip=%TEMP%\chromedriver.zip"
  set "path=%CD%\selenium\chromedriver"

  if not exist "%path%" (
    mkdir "%path%"
    if %errorlevel% neq 0 (
      echo Failed to create extraction directory: %path%
      exit /b 1
    )
  )

  echo Downloading... %1
  "%ps%" -Command "Invoke-WebRequest -Uri '%url%' -OutFile '%zip%'"
  if %errorlevel% neq 0 (
    echo Failed to download. 
    exit /b 1
  )

  echo Extracting Chromedriver to %path%...
  powershell -Command "Expand-Archive -Path '%zip%' -DestinationPath '%path%'"
  if %errorlevel% neq 0 (
      echo Failed to extract zip.
      exit /b 1
  )
  echo Chromedriver downloaded and extracted successfully to %path%.

  echo Removing temporary zip file: %zip%
  del /f "%zip%"
  if %errorlevel% neq 0 (
      echo Warning: Failed to delete the temporary zip file.
  )

  echo ChromeDriver downloaded. 
) else (
  echo Chromedriver detected, continuing...
)

echo Activating virtual environment...
call %venv_name%\Scripts\activate
python %script_name%
pause
