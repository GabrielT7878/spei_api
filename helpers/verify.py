
import time
import xarray as xr

ds = xr.open_dataset("api_output/SPEI_amazonica.nc")
print(ds)