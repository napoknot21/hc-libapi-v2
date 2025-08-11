# Heroics Capital - LibAPI

Internal API library for API connexions and data fetch

## Set up

Install the prerequirment dependencies for the lib
```bash
pip3 install -r requirement.txt
``` 
> `requirement.txt` the file in the root project


Or you can install them manually
```bash
pip3 install PACKAGE_NAME
```
`PACKAGE_NAME` : Replace this by the names of the 

### `.env` file
In order to assign values to the common parameters of the lib (i.e. `USER`, `PASSWORD`, etc) you need to create a `.env` file !

the `.env` file follows the classic structure of
```python
GLOABAL_VARIABLE="Value"
```
> Only `string` and `integer` types are accpeted
> 
> No espaces between the name, the value and the `=` sign (Convention)

Once done, place it into the lib root directory
```bash
cd src
```


## How to use / import the library ?

Core libApi is in `src` directory. You need to join the absolute path of the `libApi/src` in your project or files

```python
sys.path.append(ABSOLUTE_PATH_LIBAPI)
```
> `ABSOLUTE_PATH_LIBAPI` : str -> The absolute path of the libApi location

*Example*
```python
import pandas as pd
"""
Other imports...
"""

# Import and use of the API
sys.path.append(r"C:\path\to\the\libApi\src") # For windows paths

from libapi.ice.ice_calculator import IceCalculator
from libapi.pricers.fx import PricerFX

ice_api = IceCalculator()
pricer_fx = PricerFx()
```


## Testing

In order to run the tests, change to the `src` directory
```bash
cd src
```

Finally, using the `pytest` library, run this following
```
python3 -m pytest ..
```
> `..` : refers to the root project directory