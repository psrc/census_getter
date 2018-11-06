# census_getter
Census getter is tool to get Census 5-year ACS data using an expression file. The expression file provides information to the program- each row specifies information about which variables should be retrieved and how they should be aggregated. Currently, this tool returns data at the block group level- data at the tract level is apportioned to the block group either by total households or total population, which is also specified in the expresson file. Please see example_psrc/configs/census_getter_expressions to see how this file is set up. Also see example_psrc/configs/settings.yml for how to specify geographic area and census year. Future enhancements will provide more options for census geographies and census products.  

**Requirements:**
#. Obtain a key from the US Census Bureau by registering at http://api.census.gov/data/key_signup.html
#. Create an environmenta variable called CENSUS_KEY set to that key.
#. Create an Anaconda environment using the file example_psrc/congifs/envirnment.yml.
#. Activate environment, navigate to census_getter/example_psrc and run run_census_getter.py.

