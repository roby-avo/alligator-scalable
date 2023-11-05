import os
import pandas as pd
import time
import orjson
import sys
import traceback
from lamAPI import LamAPI


class DataPreparation:
    def __init__(self, rows, no_annotated_columns_index, lamAPI):
        self._rows = rows
        self._no_annotated_columns_index = no_annotated_columns_index
        self._lamAPI = lamAPI

  
    def compute_datatype(self):
        new_rows = []
        column_metadata = {}
        columns_data = {str(i):[] for i in range(0, len(self._rows[0]['data']))}
        target = {"SUBJ": 0, "NE": [], "LIT": [], "LIT_DATATYPE": {}, "NO_ANN": []}
        for row in self._rows:
            cells = []
            for id_col, cell in enumerate(row["data"]):
                columns_data[str(id_col)].append(str(cell))
                cells.append(self._clean_str(row["data"][id_col]))
            new_rows.append({"idRow": row["idRow"], "data": cells})    

        first_NE_column = False     
        for id_col in columns_data:
            if int(id_col) in self._no_annotated_columns_index:
                column_metadata[id_col] = "NO_ANN"
                target['NO_ANN'].append(int(id_col))
                continue
            else:
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

services = {
    "linker" : {
      "columns": ["buyer", "aug_buyer_name", "aug_url", "aug_postal_town", "aug_administrative_area_level_2", "aug_administrative_area_level_1", "aug_country"]

    },
    "classifier": {
      "columns": [["title", "description (SN)", "category"], []]
    } 
}
LAMAPI_HOST, LAMAPI_PORT = os.environ["LAMAPI_ENDPOINT"].split(":")
LAMAPI_TOKEN = os.environ["LAMAPI_TOKEN"]
lamAPI = LamAPI(LAMAPI_HOST, LAMAPI_PORT, LAMAPI_TOKEN)
time = time
file_services_name = sys.argv[1]
file_name = sys.argv[2]
kg_reference = sys.argv[3]

input_file = pd.read_csv(file_name)

with open(file_services_name, "rb") as f:
    services = orjson.loads(f.read())

rows = format_table(input_file.values.tolist())
header = list(input_file.columns)
no_annotated_columns = list(set(header) - set(services["linker"]["columns"]))
no_annotated_columns_index = [header.index(col) for col in no_annotated_columns]

column_metadata = {}
target = None
dp = DataPreparation(rows, no_annotated_columns_index, lamAPI)

output = {
    "name": file_name,
    "header": header,
    "rows": rows,
    "metadata": None,
    "target": None,
    "kg_reference": kg_reference,
    "limit": 100,
    "status": "DONE", 
    "time": time.time(),
    "services": services
}

print("Start data preparation")

try:
    if len(column_metadata) == 0:
        column_metadata, target = dp.compute_datatype()
        column_metadata[str(target["SUBJ"])] = "SUBJ"
        output["metadata"] = {
            "column": [{"idColumn": int(id_col), "tag": column_metadata[id_col]} for id_col in column_metadata]
        }
        output["target"] = target
        
    dp.rows_normalization()     
except Exception as e:
    print(f"Error {str(e)}", traceback.format_exc())


print("End data preparation")

# Writing
with open("/tmp/output.json", "wb") as f:
    f.write(orjson.dumps(output, option=orjson.OPT_INDENT_2))

print("The file has been saved correctly")
