import os
import xarray as xr
import pandas as pd

basin = "AMAZÔNICA"
cluster = 0

df = pd.read_csv("clusters_BR_bacias.csv").query("UF == @basin & KMeans_k_cotovelo == @cluster").reset_index()

points = {
    i: (row["latitude"], row["longitude"])
    for i, row in df.iterrows()
}
lats = xr.DataArray([p[0] for p in points.values()], dims="point")
lons = xr.DataArray([p[1] for p in points.values()], dims="point")

print(f"Bacia: {basin}\n\t--> Número de pontos: {len(points)}")

PREC_FILES = [
    "/home/publico/xavier_brutos/pr_19610101_19801231_BR-DWGD_UFES_UTEXAS_v_3.2.2.nc",
    "/home/publico/xavier_brutos/pr_19810101_20001231_BR-DWGD_UFES_UTEXAS_v_3.2.2.nc",
    "/home/publico/xavier_brutos/pr_20010101_20191231_BR-DWGD_UFES_UTEXAS_v_3.2.2.nc",
    "/home/publico/xavier_brutos/final_2020_ate_2024.nc"
]
TMAX_FILES = [
    "/home/publico/xavier_brutos/Tmax_19610101_19801231_BR-DWGD_UFES_UTEXAS_v_3.2.2.nc",
    "/home/publico/xavier_brutos/Tmax_19810101_20001231_BR-DWGD_UFES_UTEXAS_v_3.2.2.nc",
    "/home/publico/xavier_brutos/Tmax_20010101_20191231_BR-DWGD_UFES_UTEXAS_v_3.2.2.nc",
    "/home/publico/xavier_brutos/final_2020_ate_2024.nc"
]
TMIN_FILES = [
    "/home/publico/xavier_brutos/Tmin_19610101_19801231_BR-DWGD_UFES_UTEXAS_v_3.2.2.nc",
    "/home/publico/xavier_brutos/Tmin_19810101_20001231_BR-DWGD_UFES_UTEXAS_v_3.2.2.nc",
    "/home/publico/xavier_brutos/Tmin_20010101_20191231_BR-DWGD_UFES_UTEXAS_v_3.2.2.nc",
    "/home/publico/xavier_brutos/final_2020_ate_2024.nc"
]

all_data = {}
files_map = {'pr': PREC_FILES, 'Tmax': TMAX_FILES, 'Tmin': TMIN_FILES}

print("Iniciando o carregamento dos dados em blocos...")
for var, file_list in files_map.items():
    filtered_chunks = []
    print(f"\nProcessando variável '{var}':")

    for i, file_path in enumerate(file_list):
        try:
            ds = xr.open_dataset(file_path, decode_timedelta=False)
            data_points = ds[var].sel(latitude=lats, longitude=lons, method='nearest')
            filtered_chunks.append(data_points)
            print(f"  ✔ Bloco {i+1}/{len(file_list)} ('{os.path.basename(file_path)}') carregado e filtrado.")
            ds.close()
            del ds
        except Exception as e:
            print(f"  ❌ Erro ao processar o arquivo '{file_path}': {e}")

    if filtered_chunks:
        concatenated_data = xr.concat(filtered_chunks, dim='time')
        all_data[var] = concatenated_data
        print(f"✔ Variável '{var}' totalmente concatenada.")

final_dataset = xr.Dataset(all_data)
final_dataset = final_dataset.assign_coords(point=("point", list(points.keys())))
print("\n✅ Processamento concluído. Dataset final criado:")


print(final_dataset)
final_dataset.to_netcdf(f"data_cluster_{basin}_k{cluster}.nc")