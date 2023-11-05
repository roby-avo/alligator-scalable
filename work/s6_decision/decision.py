import sys
import orjson
import pandas as pd

K = 0.6

class Decision:
    def __init__(self, data, column_to_classify):
        self._data = data
        self._columns_to_extend = {}
        for column in column_to_classify:
            if column["target"] not in self._columns_to_extend:
                self._columns_to_extend[column["target"]] = []
            self._columns_to_extend[column["target"]].append(column["columns"])    
        

    def extract_cea_and_candidates_scored_data(self):
        cea_data = []
        candidates_scored_data = []
        
        header = []
        for id_col, col in enumerate(self._data["header"]):
            header.append(col)
            if id_col in self._data["target"]["NE"]:
                header.append(f"{self._data['kg_reference']} ID")
            for column_to_extend in self._columns_to_extend.get(col, []):
                header.append(f"{column_to_extend} categories")    

        output = []
    
        for id_row, row in enumerate(self._data["candidates"]):
            winning_candidates =  []
            cea = {}
            rankend_candidates = []
            output_cells = []
            for id_col, candidates in enumerate(row):
                output_cells.append(self._data["rows"][id_row]["data"][id_col])
                wc = []
                rank = candidates[0:20] if len(candidates) > 0 else []
                if len(candidates) > 0:
                    output_cells.append(candidates[0]["id"])
                    if len(candidates) > 1:
                        candidates[0]["delta"] = round(candidates[0]["rho2"] - candidates[1]["rho2"], 3)
                    else:
                        candidates[0]["delta"] = 1   
                    candidates[0]["score"] = round((1-K) * candidates[0]["rho2"] + K * candidates[0]["delta"], 3)
                    wc.append(candidates[0])
                    columns_categories = self._columns_to_extend.get(self._data["header"][id_col], [])
                    for column_categories in columns_categories:
                        output_cells.append(" ".join(candidates[0].get(" ".join(column_categories), [])))        
                
                if len(wc) == 1:
                    cea[str(id_col)] = wc[0]["id"]

                winning_candidates.append(wc)
                rankend_candidates.append(rank)

            cea_data.append(winning_candidates)
            candidates_scored_data.append(rankend_candidates)
            output.append(output_cells)

        return cea_data, candidates_scored_data, pd.DataFrame(output, columns=header)

   
print ("Start decision")    

filename_path = sys.argv[1]
# Reading
with open(filename_path, "rb") as f:
    input_data = orjson.loads(f.read())

decision = Decision(input_data, input_data["services"]["classifier"])
cea, candidates, output = decision.extract_cea_and_candidates_scored_data()
input_data["cea"] = cea
input_data["candidates"] = candidates

print ("End decision")

# Writing
with open("/tmp/output.json", "wb") as f:
    f.write(orjson.dumps(input_data, option=orjson.OPT_INDENT_2))

output.to_csv("/tmp/output.csv", index=False)

print ("End writing")
