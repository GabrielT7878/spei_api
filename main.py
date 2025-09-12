import requests
import xarray as xr
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import numpy as np
import time

os.makedirs("check_inputs", exist_ok=True)

def send_request(index_point, ds, scales):
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

    url = "http://localhost:8000/spei"

    payload = {
        "tmin_data": aggregate_data(ds_tmin, 'Tmin'),
        "tmax_data": aggregate_data(ds_tmax, 'Tmax'),
        "pr_data": aggregate_data(ds_prec, 'pr'),
        "lat": round(float(ds_prec['latitude'].values), 2),
        "scales": scales
    }

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

    df.to_csv(f"check_inputs/output_{index_point}.csv", index=False)

    return { 
        "df" : df,
        "status_code": f"{status_code}",   
        "index": index_point 
    }

os.makedirs("api_output", exist_ok=True)


results = []

scales = [3,12]
date_range = ("1961-01-01", "2024-12-31")


print("opening dataset")
ds = xr.open_dataset("data_cluster_AMAZÔNICA_k0_corrigido.nc")
ds = ds.sel(time=slice(*date_range))

#pontos = 2
#pontos = len(ds['point'])
pontos = [1367, 1370, 1371, 1464, 1465, 1467, 1468, 1560, 1561, 1562, 1563, 1564, 1656, 1657, 1658, 1659, 1660, 1754, 1755]
print(ds)
print(f"Total points: {pontos}")
print(f"Using {os.cpu_count()} threads for parallel requests.")
print(f"Date range: {date_range[0]} to {date_range[1]}")
print(f"Scales: {scales}")


ds_points = [ds.sel(point=x) for x in pontos]

# Execute requests in parallel
start_time = time.time()
print("Starting requests...")
with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
    futures = [executor.submit(send_request, index_point+1, ds_points[index_point], scales) for index_point in range(len(pontos))]

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
    ds[f'SPEI_{scale}'] = ds[f'SPEI_{scale}'].astype(np.float32)

ds['time'] = ds['time'].dt.floor('D')

ds.to_netcdf("api_output/SPEI_amazonica.nc")
end_time = time.time()
print(f"Results saved in {end_time - start_time:.2f} seconds.")