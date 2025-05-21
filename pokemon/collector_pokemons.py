# %%
import requests
import datetime
import json
# %%
class Collector():
    def __init__(self, url, instance_name):
        self.url = url
        self.instance_name = instance_name

    def get_response(self, **kwargs):
        response = requests.get(self.url, params=kwargs)
        return response
    
    def save_data(self, data):
        now = datetime.datetime.now().strftime("%Y%m%d_%H%m%S.%f")
        data['ingestion_date'] = datetime.datetime.now().strftime("%Y-%m-%d_%H:%m:%S.%f")

        with open(f"data/{self.instance_name}/json/{now}.json", "w") as open_file:
            json.dump(data, open_file, indent=4)

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
        
    def auto_exec(self, limit=100):
        offset = 0
        while True:
            print(offset)
            data = self.get_and_save(limit=limit, offset=offset)
            
            if data["next"] == None:
                break

            offset += limit

    def auto_exec_geracao(self):
        pokedex_ranges = [
            (1, 151),
            (152, 251),
            (252, 386),
            (387, 493),
            (494, 649),
            (650, 721),
            (722, 809),
            (810, 905),
            (906, 1025)
        ]

        for i in pokedex_ranges:
            data = self.get_and_save(offset=i[0]-1, limit=i[1]-i[0]+1)