# %%
import datetime
import json
import time
import requests
import pandas as pd
# %%
class Collector():
    def __init__(self, url, instance_name):
        self.url = url
        self.instance_name = instance_name
       
    def get_response(self, **kwargs): 
        response = requests.get(self.url, params=kwargs)

        return response

    def save_json(self, data):
        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")
        with open(f"data/{self.instance_name}/json/{now}.json", "w") as open_file:
            json.dump(data, open_file, indent=4)
    
    def save_parquet(self, data):
        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")
        df = pd.DataFrame(data)
        df.to_parquet(f"data/{self.instance_name}/parquet/{now}.parquet", index=False)

    def save_data(self, data, option="json"):
        if option == "json":
            self.save_json(data)
        elif option == "parquet":
            self.save_parquet(data)

    def get_and_save(self, save_format="json", **kwargs):
        response = self.get_response(**kwargs)

        if response.status_code == 200:
            data = response.json()
            self.save_data(data, save_format)
        else:
            data = None
            print(f"Error: {response.status_code}")

        return data
    
    def auto_exec(self, save_format="json", date_stop='2024-05-19'):
        page = 1
        date_stop = pd.to_datetime(date_stop).date()
        while True:
            print(f"Página: {page}")
            data = self.get_and_save(save_format, page=page, per_page=10)

            if data is None:
                print("Erro ao obter dados")
                time.sleep(60*5)
            else:
                date_last = pd.to_datetime(data[-1]['published_at']).date()

                if(date_last < date_stop):
                    break
                elif len(data) < 10:
                    break

                page+=1
                time.sleep(5)

#%% 
url = "https://api.jovemnerd.com.br/wp-json/jovemnerd/v1/nerdcasts/"
collector = Collector(url, "episodios")
# %%
collector.auto_exec()
# %%
