# %%
import time
import requests
import pandas as pd
import json
import datetime
# %%
def get_response(**kwargs):
    url = "https://www.tabnews.com.br/api/v1/contents"
    response = requests.get(url, params=kwargs)

    return response

def save_data(data, option="json"):
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")
    
    if option == "json":
        with open(f"data/contents/json/{now}.json", "w") as open_file:
            json.dump(data, open_file, indent=4)
    elif option == "parquet":
        df = pd.DataFrame(data)
        df.to_parquet(f"data/contents/parquet/{now}.parquet", index=False)
# %%
page = 1
date_stop = pd.to_datetime('2025-01-01').date()
while True:
    response = get_response(page=page, per_page=100, strategy="new")

    if response.status_code == 200:
        print(f"Página: {page}")
        data = response.json()
        save_data(data)
        page += 1

        date = pd.to_datetime(data[-1]['updated_at']).date()

        if len(data) < 100 or date < date_stop:
            print("Última página")
            break
    
        time.sleep(2)
    else:
        time.sleep(30)

# %%
