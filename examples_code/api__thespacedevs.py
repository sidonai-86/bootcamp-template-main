import pandas as pd
import requests


BASE_URL = "https://ll.thespacedevs.com/2.3.0/launches/"

# например: все запуски SpaceX с 2024 года
params = {
    "lsp__name": "SpaceX",         # фильтр по провайдеру запусков
    "window_start__gte": "2024-01-01",  # дата начала окна (от)
    "window_start__lte": "2025-12-31",  # дата начала окна (до)
    "ordering": "-window_start",   # сортировка по дате (новые сверху)
    "limit": 5,                    # количество объектов (макс. 100 за раз)
    "format": "json"
}

resp = requests.get(BASE_URL, params=params, timeout=30)
resp.raise_for_status()

data = resp.json()
print(data.keys())         # dict_keys(['count', 'next', 'previous', 'results'])

launches = data["results"]  # это уже список словарей

df = pd.json_normalize(launches)

df.to_csv("tmp/halltape_api.csv", sep=";", header=True, index=False)
