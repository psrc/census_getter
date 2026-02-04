# census_getter
------------
census_getter is a tool to get Census 5-year ACS data using an expression file. The expression file provides information to the program- each row specifies information about which variables should be retrieved and how they should be aggregated. Currently, this tool returns data at the block group level- data at the tract level is apportioned to the block group either by total households or total population, which is also specified in the expresson file. 


# Installation
------------
1. Install uv
    1. Open Powershell as administrator
    2. Enter the following:    
    ```
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    ```

2. Switch to CMD and clone the census_getter repository
```
git clone https://github.com/psrc/census_getter.git
```

3. Try running an example:
Starting from the census_getter directory run:
```
uv sync
.venv\Scripts\activate
cd examples\psrc
uv run run.py
```

4. Setup and run a new example project
    1. Create a new folder in census_getter\census_getter\examples
    2. Copy configs directory from another example
    3. Customize what ACS data to download in configs\census_getter_expressions.csv
    4. Customize which steps to run and other settings in configs\settings.yaml
    5. Run example using steps from above starting in the census_getter directory
    ```
    .venv\Scripts\activate
    cd examples\new_example
    uv run run.py
    ```
    6. Use -c argument if you want to use a different configs folder and settings.yaml:
    ```
    uv run run.py -c alt_configs_folder
    ```
