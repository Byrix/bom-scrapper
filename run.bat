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
)

echo Activating virtual environment...
call %venv_name%\Scripts\activate
python %script_name%
pause
