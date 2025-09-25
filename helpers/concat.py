import xarray as xr
import glob
import pandas as pd
import numpy as np

VAR = "SPI"
scales = [12]

ds_paths = glob.glob(f'/home/publico/spei_api/api_output/{VAR}*.nc')

ds_all = []
for ds_path in ds_paths:
    ds_all.append(xr.open_dataset(ds_path))

dates = ds_all[0].sel(latitude=-22,longitude=-45, method='nearest').to_dataframe().reset_index()['time']

df_finals = []

for i, ds in enumerate(ds_all):
    print(f"DATASET {i}")
    for date in dates:
        df = ds.sel(time=date).to_dataframe().reset_index().dropna(subset=f"{VAR}_1")
        df_finals.append(df)

print("Concating..")
df_concat_final = pd.concat(df_finals, ignore_index=True)

print("Transforming to nc")

ds = (
    df_concat_final.set_index(["time", "latitude", "longitude"])['SPI_12']
      .to_xarray()
)

for scale in scales:
    ds[f'{VAR}_{scale}'] = ds[f'{VAR}_{scale}'].astype(np.float32)

ds['time'] = ds['time'].dt.floor('D')

ds.to_netcdf(f"{VAR}.nc")