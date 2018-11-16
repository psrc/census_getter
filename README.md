# census_getter
census_getter is a tool to get Census 5-year ACS data using an expression file. The expression file provides information to the program- each row specifies information about which variables should be retrieved and how they should be aggregated. Currently, this tool returns data at the block group level- data at the tract level is apportioned to the block group either by total households or total population, which is also specified in the expresson file. 

Please see example_psrc/configs/census_getter_expressions to see how this file is set up. Also see example_psrc/configs/settings.yml for how to specify geographic area and census year. Future enhancements will provide more options for census geographies and census products.  

Installation
------------

1. Clone ths repository.

2. Install [Anaconda Python 2.7](https://www.continuum.io/downloads). Anaconda Python is required for census_getter.

3. Create and activate an Anaconda environment (basically a Python install just for this project)
    * Run 'conda create -n census_getter python=2.7'
    * Run 'activate census_getter' (you can re-use the environment on a later date by re-activating it or you can skip this step if you don't want to setup a new Python environment just for census_getter)
   
4. Get and install other required libraries, which can be found online.  Run the following commands on the activated conda Python environment:
    * conda install [pytables](http://www.pytables.org)
    * pip install [toolz](http://toolz.readthedocs.org/en/latest)
    * pip install [zbox](https://github.com/jiffyclub/zbox)
    * pip install [orca](https://synthicity.github.io/orca)
    * pip install [openmatrix](https://pypi.python.org/pypi/OpenMatrix)
    * pip install [activitysim](https://pypi.python.org/pypi/activitysim)
    * pip install [ortools](https://github.com/google/or-tools)
    * pip install [synthpop](https://github.com/UDST/synthpop)
    * pip install https://github.com/RSGInc/populationsim/zipball/master

6. Set the working_dir in the file `census_getter/example_psrc/run_census_getter.py` to the root of the cloned repository.

7. Run census_getter.
    * Navigate to census_getter/example_psrc directory. 
    * Run`python run_census_getter.py`
    
8. Outputs are located in example_psrc/output.
