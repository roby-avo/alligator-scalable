import os
import pandas as pd
import time
import json
from lamAPI import LamAPI


class DataPreparation:
    def __init__(self, rows, lamAPI):
        self._rows = rows
        self._lamAPI = lamAPI

  
    def compute_datatype(self):
        new_rows = []
        column_metadata = {}
        columns_data = {str(i):[] for i in range(0, len(self._rows[0]['data']))}
        target = {"SUBJ": 0, "NE": [], "LIT": [], "LIT_DATATYPE": {}}
        for row in self._rows:
            cells = []
            for id_col, cell in enumerate(row["data"]):
                columns_data[str(id_col)].append(str(cell))
                cells.append(self._clean_str(row["data"][id_col]))
            new_rows.append({"idRow": row["idRow"], "data": cells})    

        first_NE_column = False     
        for id_col in columns_data:
            metadata = self._lamAPI.literal_recognizer(columns_data[id_col])
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

    
    def rows_normalization(self):
        for row in self._rows:
            for id_col, cell in enumerate(row["data"]):
                row["data"][id_col] = self._clean_str(row["data"][id_col])

                
    def _clean_str(self, value):
        value = str(value)
        stop_charaters = ["_"]
        for char in stop_charaters:
            value = value.replace(char, " ")
        value = " ".join(value.split()).lower()
        return value



def format_table(table_df):
    rows = []
    for id_row, row in enumerate(table_df):
        rows.append({"idRow":id_row+1, "data": row})
  
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
    output["metadata"] = {
        "column": [{"idColumn": int(id_col), "tag": column_metadata[id_col]} for id_col in column_metadata]
    }
    output["target"] = target
    
dp.rows_normalization()     

with open("/tmp/output.json", "w") as f:
    f.write(json.dumps(output, indent=4)) 
