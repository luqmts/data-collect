# %%
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
# %%
def get_content(url):
    headers = {
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
    response = requests.get(url, headers=headers)
    return response

def get_basic_info(soup):
    ems = (soup
        .find("div", class_="td-page-content")
        .find_all("p")[1]
        .find_all("em"))

    data = {}
    for i in ems:
        chave, valor, *_ = i.text.split(":")
        chave = chave.strip(" ")
        data[chave] = valor.strip(" ")

    return data

def get_aparicoes(soup):
    lis = (soup.find("div", class_="td-page-content")
        .find("h4")
        .find_next()
        .find_all("li"))
    aparicoes = [i.text for i in lis]
    return aparicoes

def get_personagem_info(url):
    response = get_content(url)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        data = get_basic_info(soup)
        data["Aparicoes"] = get_aparicoes(soup)

        return data
    else:
        print("Não foi possível acessar a página.")
        return {}

def get_personagens():
    url = "https://www.residentevildatabase.com/personagens"
    response = get_content(url)
    soup_personagens = BeautifulSoup(response.text, "html.parser")
    ancoras = (soup_personagens
            .find("div", class_="td-page-content")
            .find_all("a"))

    links = [i["href"] for i in ancoras]
    return links
# %%
links = get_personagens()
data = []

for i in tqdm(links):
    d = get_personagem_info(i)
    d['Link'] = i
    nome = i.strip("/").split("/")[-1].replace("-", " ")
    d['Nome'] = nome
    data.append(d)

data
# %%
df = pd.DataFrame(data)
df.to_parquet("data/dados_re.parquet", index=False)
df.to_pickle("data/dados_re.pkl")
# %%
df_parquet = pd.read_parquet("data/dados_re.parquet")
df_pickle = pd.read_pickle("data/dados_re.pkl")
df_pickle
# %%
