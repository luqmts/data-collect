# %%
from pokemon import (
    collector_pokemons, 
    collector_pokemon_details,
    treatment_pokemon
)
from dotenv import load_dotenv
import os
# %%
# pokemon data collector
collector = collector_pokemons.Collector("https://pokeapi.co/api/v2/pokemon", "pokemon")
collector.auto_exec()
# %%
# pokemon details data collector
collector = collector_pokemon_details.Collector("pokemon_details")
processor = collector_pokemon_details.Processor('./pokemon/data/pokemon/json/', collector)
processor.build_dataframe()
processor.run_multiprocessing()
# %%
load_dotenv()
options = {
    "user": os.getenv('SQL_LOGIN'),
    "password": os.getenv('SQL_PASS'),
    "host": "localhost",
    "port": 3306,
    "database": "pokemon"
}
file_path = './pokemon/data/pokemon_details/json'

pkmdataprocessor = treatment_pokemon.PokemonDataProcessor(file_path=file_path, db_config=options)
pkmdataprocessor.load_and_persist('pokemon')
