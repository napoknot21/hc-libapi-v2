# Heroics Capital - LibAPI

Internal API library for API connexions and data fetch

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

from libApi.ice.ice_calculator import IceCalculator
from libApi.pricers.fx import PricerFX

ice_api = IceCalculator()
pricer_fx = PricerFx()
```
