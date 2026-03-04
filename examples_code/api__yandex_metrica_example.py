import os
import requests
import pandas as pd

TOKEN = "y0__xDe_Im4BBiirjkg2Zzs_xP3srDDcLIeER6ns2I71_wDiFmYXg"
COUNTER_ID = 103580753

headers = {
    "Authorization": f"OAuth {TOKEN}"
}

metrics = [
    "ym:s:visits",
    "ym:s:users",
    "ym:s:newUsers",
    "ym:s:pageviews"
]

params = {
    "date1": "2025-08-01",
    "date2": "2025-08-10",
    "ids": COUNTER_ID,
    "metrics": ",".join(metrics),
    "dimensions": "ym:s:date"
}

url = "https://api-metrika.yandex.net/stat/v1/data"

response = requests.get(url, headers=headers, params=params)
data = response.json()

dimension_names = data["query"]["dimensions"]
metric_names = data["query"]["metrics"]

# Строим список строк
rows = []
for row in data["data"]:
    dim_values = [d["name"] for d in row["dimensions"]]
    metrics = row["metrics"]
    rows.append(dim_values + metrics)

# Названия колонок
columns = dimension_names + metric_names

# Финальный DataFrame
df = pd.DataFrame(rows, columns=columns)

print(df)
