# census_getter
Census getter is tool to get Census 5-year ACS data using an expression file. The expression file provides information to the program- each row specifies information about which variables should be retrieved and how they should be aggregated. Currently, this tool returns data at the block group level- data at the tract level is apportioned to the block group either by total households or total population, which is also specified in the expresson file. 

Please see example_psrc/configs/census_getter_expressions to see how this file is set up. Also see example_psrc/configs/settings.yml for how to specify geographic area and census year. Future enhancements will provide more options for census geographies and census products.  

**Requirements:**
1. Obtain a key from the US Census Bureau by registering at http://api.census.gov/data/key_signup.html
2. Create an environment variable called CENSUS_KEY set to that key.
3. Create an [Anaconda environment](https://conda.io/docs/user-guide/tasks/manage-environments.html#creating-an-environment-from-an-environment-yml-file) using the file example_psrc/congifs/environment.yml. The config file generates a new environment called **popsim**
    * `conda env create -f example_psrc\configs\environment.yml`
4. Set the working_dir in the file `census_getter/example_psrc/run_census_getter.py` to the root of the cloned repository.
5. Activate the environment, navigate to census_getter/example_psrc, and start run_census_getter.py.
    * `activate popsim`
    * `python run_census_getter.py`
6. Outputs are located in example_psrc/output.
