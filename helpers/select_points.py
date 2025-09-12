import requests
import xarray as xr
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import numpy as np
import time

ds = xr.open_dataset("data_cluster_AMAZÔNICA_k0_corrigido.nc")

pontos = [
    (-7.65, -58.45),
    (-7.55, -58.45),
    (-7.45, -58.45),
    (-7.35, -58.45),
    (-7.55, -58.35),
    (-7.45, -58.35),
    (-7.35, -58.35),
    (-7.45, -58.25),
    (-7.35, -58.25),
    (-7.25, -58.25),
    (-7.65, -58.15),
    (-7.55, -58.15),
    (-7.45, -58.15),
    (-7.35, -58.15),
    (-7.25, -58.15),
    (-7.65, -58.05),
    (-7.55, -58.05),
    (-7.45, -58.05),
    (-7.35, -58.05),
]

pontos_str = [f"{lat:.2f},{lon:.2f}" for lat, lon in pontos]

df = ds.sel(time="1961-01-01").to_dataframe().dropna(subset='pr').reset_index()
df['lat_lon'] = df.apply(lambda row: f"{row['latitude']:.2f},{row['longitude']:.2f}", axis=1)
df = df[df['lat_lon'].isin(pontos_str)]
valores_int = [int(v) for v in df['point'].values]
print(valores_int)