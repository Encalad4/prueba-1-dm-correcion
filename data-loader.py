import pandas as pd
import time

mg_urls = [
    mg_users_url,
    mg_plans_url,
    mg_calls_url,
    mg_messages_url,
    mg_internet_url
]

mg_data_tables = []

MAX_RETRIES = 5
CHUNK_SIZE = 10000

for url in mg_urls:
    print(f"Cargando: {url}")

    for attempt in range(MAX_RETRIES):
        try:
            # Chunking real
            chunk_iterator = pd.read_csv(url, chunksize=CHUNK_SIZE)

            for chunk in chunk_iterator:
                mg_data_tables.append(chunk)

            print(f"Carga exitosa: {url}")
            break  # salir del retry loop si fue exitoso

        except Exception as e:
            wait_time = 2 ** attempt  # exponential backoff
            print(f"Error en intento {attempt + 1}: {e}")
            print(f"Reintentando en {wait_time}s...")

            time.sleep(wait_time)

            if attempt == MAX_RETRIES - 1:
                raise Exception(f"Fallo definitivo cargando {url}")