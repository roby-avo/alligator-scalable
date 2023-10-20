import os
import numpy as np
import pandas as pd
import math
import time
from lamAPI import LamAPI


def clean_str(value):
    value = str(value)
    stop_charaters = ["_"]
    for char in stop_charaters:
        value = value.replace(char, " ")
    value = " ".join(value.split()).lower()
    return value


def format_table(table_df):
    rows = []
    for id_row, row in enumerate(table_df):
        rows.append({"idRow":id_row+1, "data":[clean_str(cell) for cell in row]})
  
    return rows


LAMAPI_HOST, LAMAPI_PORT = os.environ["LAMAPI_ENDPOINT"].split(":")
LAMAPI_TOKEN = os.environ["LAMAPI_TOKEN"]
lamAPI = LamAPI(LAMAPI_HOST, LAMAPI_PORT, LAMAPI_TOKEN)
time = time
name = "film.csv"
input_file = pd.read_csv(name)
rows = format_table(input_file.values.tolist())
header = list(input_file.columns)
kg_reference = "wikidata"
column_metadata = {}
target = None
dp = DataPreparation(rows, lamAPI)

output = {
    "name": name,
    "header": header,
    "rows": rows,
    "metadata": None,
    "target": None,
    "kg_reference": kg_reference,
    "limit": 100,
    "status": "DONE", 
    "time": time.time()
}

if len(column_metadata) == 0:
    column_metadata, target = dp.compute_datatype()
    column_metadata[str(target["SUBJ"])] = "SUBJ"
    #output["column"] = column_metadata
    output["metadata"] = {
        "column": [{"idColumn": int(id_col), "tag": column_metadata[id_col]} for id_col in column_metadata]
    }
    output["target"] = target
    
dp.rows_normalization()     
