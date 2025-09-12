
import time
import xarray as xr

ds = xr.open_dataset("data_cluster_AMAZÔNICA_k0.nc")
diff = ds['Tmax'] - ds['Tmin']
tmax_corr = ds['Tmax'].where(diff >= 0)
tmin_corr = ds['Tmin'].where(diff >= 0)

tmax_interp = tmax_corr.interpolate_na(dim="time", method="linear")
tmin_interp = tmin_corr.interpolate_na(dim="time", method="linear")

tmean = (tmax_interp + tmin_interp) / 2

ds['pr'] = ds['pr'].fillna(0)
ds.to_netcdf("data_cluster_AMAZÔNICA_k0_corrigido.nc")