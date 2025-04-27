@echo off
REM Check if the conda environment 'scrapper' exists
conda env list | findstr /B "bom-scrapper" > nul 2>&1

REM If the environment does not exist, create it from the environment.yml file
if errorlevel 1 (
    echo Creating conda environment 'bom-scrapper'...
    conda env create -f conda-env.yml -y
    if errorlevel 1 (
        echo Error creating conda environment. Please check environment.yml.
        exit /b 1
    )
) else (
    echo Conda environment 'bom-scrapper' already exists.
)

REM Activate the conda environment
echo Activating conda environment 'bom-scrapper'...
call conda activate bom-scrapper
if errorlevel 1 (
    echo Error activating conda environment 'bom-scrapper'.
    exit /b 1
)

REM Run the python script
echo Running python script...
python bom_scrapper.py

REM Deactivate the conda environment (optional)
echo Deactivating conda environment 'scrapper'...
call conda deactivate

echo Script finished.
exit /b 0