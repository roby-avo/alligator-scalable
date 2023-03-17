import os
import numpy as np
import pandas as pd
import math
from lamAPI import LamAPI


SAMPLE_SIZE = 25
LAMAPI_HOST, LAMAPI_PORT = os.environ["LAMAPI_ENDPOINT"].split(":")
LAMAPI_TOKEN = os.environ["LAMAPI_TOKEN"]

def format_table(table_df, header, kg_reference="wikidata"):
    rows = []

    for id_row, row in enumerate(table_df):
        rows.append({"idRow":id_row+1, "data":row})
    print("rows", rows)
    print("SAMPLE_SIZE", SAMPLE_SIZE)
    print(math.floor(len(rows)/SAMPLE_SIZE))
    #buffer = []
    #rows = np.array_split(rows, max(math.floor(len(rows)/SAMPLE_SIZE), 1))

        
    object = {
        "header": header,
        "rows": rows,
        "kgReference": kg_reference
    }
        
    return object


def compute_datatype(rows, lamapi_wrapper):
    column_metadata = {}
    columns_data = {str(i):[] for i in range(0, len(rows[0]['data']))}
    target = {"SUBJ": 0, "NE": [], "LIT": [], "LIT_DATATYPE": {}}
    for row in rows:
        for id_col, cell in enumerate(row["data"]):
            columns_data[str(id_col)].append(str(cell))
    
    first_NE_column = False     
    for id_col in columns_data:
        metadata = lamapi_wrapper.literal_recognizer(columns_data[id_col])
        max_datatype = max(metadata, key=metadata.get)
        if max_datatype == "ENTITY":
            column_metadata[id_col] = "NE"
            target['NE'].append(int(id_col)) 
            if not first_NE_column:
                target["SUBJ"] = int(id_col)
            first_NE_column = True
        else:
            column_metadata[id_col] = "LIT"
            target['LIT'].append(int(id_col))
            target['LIT_DATATYPE'][str(id_col)] = max_datatype
            
    return column_metadata, target        


def pre_processing(header, rows, column_metadata, types, candidate_size):
    cells_buffer = {}

    for cell in header:
        if cell == '':
            continue
        cell = cell.lower()
        if cell not in cells_buffer:
            cells_buffer[cell] =  {}
        cells_buffer[cell][None] = True

    for row in rows: 
        for i, cell in enumerate(row["data"]):
            if cell == '' or ("cellToSize" in row and row["cellToSize"][cell] != candidate_size):
                continue
            if column_metadata[str(i)] == 'NE' or column_metadata[str(i)] == 'SUBJ':
                cell = str(cell).lower()
                if cell not in cells_buffer:
                    cells_buffer[cell] =  {}
                type = types.get(str(i))
                cells_buffer[cell][type] = True
    cells_set = []
    for cell in cells_buffer:
        for type in cells_buffer[cell]:
            cells_set.append({"cell": cell, "type": type})
    return cells_set



lamAPI = LamAPI(LAMAPI_HOST, LAMAPI_PORT, LAMAPI_TOKEN)

input = pd.read_csv("film.csv")
data = format_table(input.values.tolist(), list(input.columns))
header = data.get("header", [])
rows = data["rows"]
kg_reference = data["kgReference"]
column_metadata = {}
target = None


obj_row_update = {"status": "DONE", "time": None}

if len(column_metadata) == 0:
        column_metadata, target = compute_datatype(rows, lamAPI)
        column_metadata[str(target["SUBJ"])] = "SUBJ"
        obj_row_update["column"] = column_metadata
        obj_row_update["metadata"] = {
            "column": [{"idColumn": int(id_col), "tag": column_metadata[id_col]} for id_col in column_metadata]
        }
        obj_row_update["target"] = target

cells_set = pre_processing(header, rows, column_metadata, {}, 100)


print("WELL DONE!")
print(data)

df = pd.DataFrame(data, index=[0])
df.to_parquet('myfile.parquet')
