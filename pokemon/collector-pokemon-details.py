# %%
import requests
import pandas as pd
import os
import json
import datetime
from multiprocessing import Pool
# %%
class Collector():
    def __init__(self, instance_name):
        self.instance_name = instance_name

    def get_response(self, url):
        response = requests.get(url)
        return response
    
    def save_data(self, data):
        now = datetime.datetime.now().strftime("%Y%m%d_%H%m%S.%f")
        data['ingestion_date'] = datetime.datetime.now().strftime("%Y-%m-%d_%H:%m:%S.%f")

        with open(f"data/{self.instance_name}/json/{data['id']}_{now}.json", "w") as open_file:
            json.dump(data, open_file, indent=4)

    def get_and_save(self, url):
        response = self.get_response(url)

        if response.status_code == 200:
            data = response.json()
            self.save_data(data)
            print(f"Data saved successfully!")
            return data
        else:
            print(f"Error: {response.status_code}")
            return {}

class Processor():
    def __init__(self, folder_path, collector_instance):
        self.folder_path = folder_path
        self.collector = collector_instance
        self.df = pd.DataFrame()

    def load_json_files(self):
        data = []
        json_files = os.listdir(self.folder_path)

        for file_name in json_files:
            file_path = os.path.join(self.folder_path, file_name)

            with open(file_path) as f:
                file = json.load(f)
                data.append(file)

        return data
    
    def build_dataframe(self):
        data = self.load_json_files()
        df = pd.DataFrame(data)
        df = df.explode('results')
        df[['name', 'url']] = df['results'].apply(pd.Series)
        df = df[['ingestion_date', 'name', 'url']]
        self.df = df

        return df
    
    def run_multiprocessing(self, num_processes=5):
        if self.df.empty:
            self.build_dataframe()

        if __name__ == '__main__':
            with Pool(num_processes) as p:
                p.map(self.collector.get_and_save, self.df['url'])
# %%
collector = Collector("pokemon_details")
processor = Processor('./data/pokemon/json/', collector)
processor.build_dataframe()
processor.run_multiprocessing()