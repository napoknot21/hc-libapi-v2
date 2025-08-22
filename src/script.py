import os, sys
import datetime as dt

sys.path.append(os.getcwd())
from libapi.ice import IceCalculator

ic = IceCalculator()

res = ic.run_im_bilateral(dt.datetime.now())
print(res)
ic.get_post_im("2025-07-21")
