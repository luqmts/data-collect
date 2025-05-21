# %%
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
# %%
class Collector():
    def __init__(self, url):
        self.url = url
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:138.0) Gecko/20100101 Firefox/138.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.residentevildatabase.com/personagens/',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Priority': 'u=0, i',
        }

    def get_response(self, url):
        response = requests.get(url, headers=self.headers)
        return response
    
    def get_basic_info(self, soup):
        data = {}
        ems = (soup
            .find("div", class_="td-page-content")
            .find("h4")
            .find_previous_sibling().text
        ).split('\n')

        for i in ems:
            chave, valor = i.split(":")
            chave = chave.strip(" ")
            data[chave] = valor.strip(" ")

        return data
    
    def get_aparicoes(self, soup):
        lis = (soup.find("div", class_="td-page-content")
            .find("h4")
            .find_next()
            .find_all("li"))
        aparicoes = [i.text for i in lis]

        return aparicoes

    def get_personagem_info(self, url_personagem):
        response = self.get_response(url_personagem)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            data = self.get_basic_info(soup)
            data["Aparicoes"] = self.get_aparicoes(soup)
            return data
        else:
            print("Não foi possível acessar a página.")
            return {}
    
    def get_personagens(self):
        response = self.get_response(self.url)
        soup_personagens = BeautifulSoup(response.text, "html.parser")
        ancoras = (soup_personagens
                .find("div", class_="td-page-content")
                .find_all("a"))
        links = [i["href"] for i in ancoras]
        
        return links
    
    def save_data(self, data):
        df = pd.DataFrame(data)
        path_file = "data/dados_re"
        df.to_parquet(f"{path_file}.parquet", index=False)
        df.to_pickle(f"{path_file}.pkl")

        print(f"Data saved successfully to {path_file}.parquet and {path_file}.pkl")

    def auto_exec(self):
        links = self.get_personagens()
        data = []
        for i in tqdm(links):
            d = self.get_personagem_info(i)
            d['Link'] = i
            nome = i.strip("/").split("/")[-1].replace("-", " ").title()
            d['Nome'] = nome
            data.append(d)

        self.save_data(data)
        print("Data collection completed.")
        return data

# %%
collector = Collector("https://www.residentevildatabase.com/personagens/")
data = collector.auto_exec()