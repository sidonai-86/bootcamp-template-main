mkdir -p data && \
curl -L -o data/google_users.csv   "https://huggingface.co/datasets/halltape/users/resolve/main/google_users.csv" && \
curl -L -o data/other_users.csv    "https://huggingface.co/datasets/halltape/users/resolve/main/other_users.csv" && \
curl -L -o data/yandex_users.csv   "https://huggingface.co/datasets/halltape/users/resolve/main/yandex_users.csv"