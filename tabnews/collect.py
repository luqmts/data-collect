# %%
import time
import requests
import pandas as pd
import json
import datetime
# %%
class Collector():
    def __init__(self, url):
        self.url = url

    def get_response(self, **kwargs):
        response = requests.get(self.url, params=kwargs)
        return response

    def save_data(self, data, option="json"):
        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")
        
        if option == "json":
            with open(f"data/contents/json/{now}.json", "w") as open_file:
                json.dump(data, open_file, indent=4)
        elif option == "parquet":
            df = pd.DataFrame(data)
            df.to_parquet(f"data/contents/parquet/{now}.parquet", index=False)

    def get_and_save(self, **kwargs):
        response = self.get_response(**kwargs)

        if response.status_code == 200:
            data = response.json()
            self.save_data(data)
            print(f"Data saved successfully!")
            return data
        else:
            print(f"Error: {response.status_code}")
            return {}
        
    def auto_exec(self, date_stop_str='2025-01-01', **kwargs):
        page = 1
        date_stop = pd.to_datetime(date_stop_str).date()

        while True:
            response = self.get_response(page=page, **kwargs)

            if response.status_code == 200:
                print(f"Página: {page}")

                data = response.json()
                self.save_data(data)
                page += 1

                date = pd.to_datetime(data[-1]['updated_at']).date()
                
                print(f"{date}{date_stop}")
                print(date < date_stop)
                print(len(data))

                if len(data) < kwargs.get('per_page', 100) or date < date_stop:
                    print("Última página")
                    break
            
                time.sleep(2)
            else:
                time.sleep(30)
# %%
url = "https://www.tabnews.com.br/api/v1/contents"
collector = Collector(url)
collector.auto_exec(per_page=100, strategy="new")
# %%
