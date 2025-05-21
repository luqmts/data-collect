# %%
import pandas as pd
import os
import json
from sqlalchemy import create_engine
from dotenv import load_dotenv
from tqdm import tqdm
# %%
class PokemonDataProcessor:
    def __init__(self, file_path: str, db_config: dict):
        self.file_path = file_path
        self.db_config = db_config
        self.engine = self._create_engine()
        self.df = None
    
    def _create_engine(self):
        return create_engine(
            f"mysql+pymysql://{self.db_config['user']}:{self.db_config['password']}"
            f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
        )

    def _extract_stats(self, stats_list):
        return {i['stat']['name']: i['base_stat'] for i in stats_list}

    def _extract_types(self, types_list):
        return {f'type {i['slot']}':i['type']['name'] for i in types_list}

    def load_data(self, limit=None):
        files = os.listdir(self.file_path)
        data = []

        if limit:
            files = files[:limit]

        for i in tqdm(files):
            with open(f"{self.file_path}/{i}") as f:
                file = json.load(f)
                data.append(file)
        
        self.df = pd.DataFrame(data)

    def process_data(self):
        stats_df = self.df['stats'].apply(self._extract_stats).apply(pd.Series)
        types_df = self.df['types'].apply(self._extract_types).apply(pd.Series)

        self.df = pd.concat([
            self.df[['ingestion_date', 'order', 'name', 'weight', 'height' ]],
            types_df,
            stats_df
        ], axis= 1)

    def save_to_sql(self, table_name: str, if_exists='replace'):
        if self.df is not None:
            self.df.to_sql(table_name, con=self.engine, index=False, if_exists=if_exists)
        else:
            print("Dataframe está vazio, preencha antes de tentar subir")

    def load_and_persist(self, table_name: str, if_exists='replace'):
        self.load_data()

        if self.df is not None: 
            self.process_data()
            self.save_to_sql(table_name=table_name, if_exists=if_exists)

            print("Executado com sucesso!")