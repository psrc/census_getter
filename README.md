# census_getter
------------
census_getter is a tool to get Census 5-year ACS data using an expression file. The expression file provides information to the program- each row specifies information about which variables should be retrieved and how they should be aggregated. Currently, this tool returns data at the block group level- data at the tract level is apportioned to the block group either by total households or total population, which is also specified in the expresson file. 


# Installation
------------
1. Clone ths repository.

2. Customize what ACS data to download in data/census_getter_expressions.csv

3. Customize which steps to run and other settings in settings.yaml

4. Navigate into the census_getter directory and use uv to create the venv and run the application
```
cd census_getter\census_getter
uv run run.py
```