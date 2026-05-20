# census_getter
------------
census_getter is a tool to get Census 5-year ACS data using an expression file. The expression file provides information to the program- each row specifies information about which variables should be retrieved and how they should be aggregated. Currently, this tool returns data at the block group level- data at the tract level is apportioned to the block group either by total households or total population, which is also specified in the expresson file. 


# Installation
------------
1. Setup census api key as an env variable
    - Get a free census api key here: https://api.census.gov/data/key_signup.html
    - Save your api key as an env variable named CENSUS_KEY
    
2. Install uv
    1. Open Powershell as administrator
    2. Enter the following:    
    ```
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    ```

3. Switch to CMD and clone the census_getter repository
    ```
    git clone https://github.com/psrc/census_getter.git
    ```

4. Try running an example:
Starting from the census_getter directory run:
    ```
    uv run census_getter\run.py -c examples\population_synthesizer\configs_census_getter
    ```

5. Setup and run a new example project
    1. Create a new folder in census_getter\examples
    2. Copy configs directory from another example
    3. Customize what ACS data to download in configs_census_getter\census_getter_expressions.csv
    4. Customize which steps to run and other settings in configs_census_getter\settings.yaml
    5. Run example using steps from above starting in the census_getter directory
        ```
        uv run census_getter\run.py -c examples\<example_dir>\configs_census_getter
        ```
    6. Use -c argument if you want to use a different configs folder and settings.yaml:
        ```
        uv run census_getter\run.py -c <path to configs folder>
        ```

6. Run populationsim as a step within pypyr pipeline
    1. Copy the configs_popsim folder to your project
    2. Make sure in configs_census_getter/settings.yaml that `configs_popsim` points to the popsim configs directory
