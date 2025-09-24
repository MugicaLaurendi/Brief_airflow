import os
import duckdb
from pathlib import Path

def init_database(DUCK_DATABASE):

    my_file = Path(DUCK_DATABASE)
    
    if my_file.is_file():
        os.remove(DUCK_DATABASE)

    duckdb.connect(DUCK_DATABASE)
    print("-------  DUCK DATABASE CREATED --------")

def remove_database(DUCK_DATABASE):

    os.remove(DUCK_DATABASE)
    print("-------  DUCK DATABASE REMOVED --------")