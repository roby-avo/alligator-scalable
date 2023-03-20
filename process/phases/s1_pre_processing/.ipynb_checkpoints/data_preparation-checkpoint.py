
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