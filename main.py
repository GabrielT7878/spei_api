import requests
import xarray as xr
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import numpy as np
import time
import socket

os.makedirs("check_inputs", exist_ok=True)

scales = [1,12]
date_range = ("1993-01-01", "2024-03-20")
VAR = "SPI"
USE_CLUSTERS = True

def send_request(index_point, ds, scales):
    print("entry send request")
    ds_tmax = ds['Tmax']
    ds_tmin = ds['Tmin']
    ds_prec = ds['pr']


    def aggregate_data(ds, var):
        df = ds.to_dataframe().reset_index()
        df.to_csv(f"check_inputs/input_{var}_{index_point}.csv", index=False)
        df["year_month"] = df["time"].dt.to_period("M")
        if var == "pr":
            df_mensal = df.groupby("year_month")[var].sum().reset_index()
        else:
            df_mensal = df.groupby("year_month")[var].mean().reset_index()
        return df_mensal[var].values.tolist()


    url = f"http://localhost:8000/{VAR.lower()}"

    if VAR == "SPEI":

        payload = {
            "tmin_data": aggregate_data(ds_tmin, 'Tmin'),
            "tmax_data": aggregate_data(ds_tmax, 'Tmax'),
            "pr_data": aggregate_data(ds_prec, 'pr'),
            "lat": round(float(ds_prec['latitude'].values), 2),
            "scales": scales
        }

    elif VAR == "SPI":
        payload = {
            "pr_data": aggregate_data(ds_prec, 'pr'),
            "scales": scales
        }
    
    else:
        raise Exception(f"Varível Inválida ({VAR}): deve ser uma das seguintes: SPEI, SPI")


    response = requests.post(url, json=payload)
    status_code = response.status_code

    if response.status_code != 200:
        print(f"Request failed with status code {status_code} for index {index_point}")
        return { 
            "status_code": f"{status_code}",
            "index": index_point,
            "body" : response.text
        }

    results = response.json()

    initial_date = str(ds['time'].min().values)[:10]
    end_date = str(ds['time'].max().values)[:10]


    dates = pd.period_range(initial_date, end=end_date, freq='M').to_timestamp(how='end')
    dates = dates.normalize()
    #caso nao seja o ultimo dia do mes substitui pelo end_date
    dates = dates[:-1].append(pd.to_datetime([end_date]))

    df_dict = {
        "time": dates,
        "latitude": round(float(ds_prec['latitude'].values), 2),
        "longitude": round(float(ds_prec['longitude'].values), 2),
    }

    # Adiciona dinamicamente os SPEIs
    for key, values in results.items():
        df_dict[key] = [x if x != "NA" else np.nan for x in values]

    df = pd.DataFrame(df_dict)

    return { 
        "df" : df,
        "status_code": f"{status_code}",   
        "index": index_point 
    }

os.makedirs("api_output", exist_ok=True)


results = []

print("opening dataset")
file_name = "base_concatenada2.zarr"
base_path = "/mnt/nfs_mount/" if socket.gethostname() != "irbrerd09" else "/home/publico/"
path = base_path + file_name

ds = xr.open_zarr(path)
ds = ds.sel(time=slice(*date_range))

valid_points = pd.read_csv("valid_points.csv")

batch_size_per_machine = {
    #"irbrerd02" : (0,0),
    "irbrerd03" : (0,0),
    #"irbrerd06" : (0,0),
    "irbrerd08" : (0,0),
    "irbrerd09" : (0,0),
}

if not USE_CLUSTERS:
    batch_size_per_machine = {
        "irbrerd09" : (0,0),
    }

start = 0
for key, value in batch_size_per_machine.items():
    batch_size_per_machine[key] = (start,start+(len(valid_points) // len(batch_size_per_machine.keys())))
    start += (len(valid_points) // len(batch_size_per_machine.keys()))

if len(valid_points) % len(batch_size_per_machine.keys()):
    new_assign = batch_size_per_machine['irbrerd09'][0]
    batch_size_per_machine['irbrerd09'] = (new_assign,len(valid_points))

print(batch_size_per_machine)

hostname = socket.gethostname()
print("Escolhido:", hostname, " ", batch_size_per_machine[hostname])

#total_pontos = 100
batch_range = batch_size_per_machine[hostname]
#batch_range = (0,2)
print(f"batch range: {batch_range}")
valid_points = valid_points[batch_range[0]:batch_range[1]]
total_pontos = len(valid_points)
print(ds)
print(f"Total points: {total_pontos}")
print(f"Using {os.cpu_count()} threads for parallel requests.")
print(f"Date range: {date_range[0]} to {date_range[1]}")
print(f"Scales: {scales}")
print("VAR:", VAR)
print("Using Clusters:", USE_CLUSTERS)

print("selecting points...")
ds_points = [ds.sel(latitude=row['latitude'], longitude=row['longitude']) for i, row in valid_points[:total_pontos].iterrows()]

# Execute requests in parallel
start_time = time.time()
print("Starting requests...")
with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
    futures = [executor.submit(send_request, index_point+1, ds_points[index_point], scales) for index_point in range(total_pontos)]

    for future in as_completed(futures):
        results.append(future.result())
        print(f"Request {future.result()['index']} completed with status: {future.result()['status_code']}")

end_time = time.time()
print(f"All requests completed in {end_time - start_time:.2f} seconds.")

print("Saving results...")
start_time = time.time()

list_results_df = [result['df'] for result in results if 'df' in result]
df = pd.concat(list_results_df, ignore_index=True)

ds = (
    df.set_index(["time", "latitude", "longitude"])
      .to_xarray()
)

for scale in scales:
    ds[f'{VAR}_{scale}'] = ds[f'{VAR}_{scale}'].astype(np.float32)

ds['time'] = ds['time'].dt.floor('D')

ds.to_netcdf(f"api_output/{VAR}_{hostname}.nc")
end_time = time.time()
print(f"Results saved in {end_time - start_time:.2f} seconds.")