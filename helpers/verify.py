
import time
import xarray as xr
import pandas as pd

ds = xr.open_dataset("/home/publico/spei_api/helpers/SPI_12.nc")
#print(ds.sel(time='1963-01-31').to_dataframe().reset_index().dropna(subset="SPI_1"))
print(ds)

# df = pd.read_csv("/home/gfernandes/Projetos/spei_api/check_inputs/output_1.csv")
# print(df[df['time'] >= "2024-01-01"]['prec_mensal'].sum())

# import xarray as xr

# Abrir um arquivo .zarr
# z = xr.open_zarr("/home/publico/base_concatenada2.zarr")
# valor = z['pr'].sel(latitude=-7.65, longitude=-58.45, method="nearest")
# print(valor)

# valid_points = pd.read_csv("valid_points.csv")
# print(valid_points.iloc[0]['latitude'])