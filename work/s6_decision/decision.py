import sys
import orjson
import pandas as pd

K = 0.6
SIGMA = 0.5

class Decision:
    def __init__(self, data):
        self._data = data
        
    def extract_cea_and_candidates_scored_data(self):
        cea_data = []
        candidates_scored_data = []
            
        for id_row, row in enumerate(self._data["candidates"]):
            winning_candidates =  []
            cea = {}
            rankend_candidates = []
            for id_col, candidates in enumerate(row):
                wc = []
                rank = candidates[0:20] if len(candidates) > 0 else []
                if len(candidates) > 0:
                    if len(candidates) > 1:
                        candidates[0]["delta"] = round(candidates[0]["rho2"] - candidates[1]["rho2"], 3)
                    else:
                        candidates[0]["delta"] = 1   
                    candidates[0]["score"] = round((1-K) * candidates[0]["rho2"] + K * candidates[0]["delta"], 3)
                    wc.append(candidates[0])
                        
                if len(wc) == 1:
                    cea[str(id_col)] = wc[0]["id"]
                    if wc[0]["score"] > SIGMA:
                        wc[0]["match"] = True

                winning_candidates.append(wc)
                rankend_candidates.append(rank)

            cea_data.append(winning_candidates)
            candidates_scored_data.append(rankend_candidates)

        return cea_data, candidates_scored_data
    

    def get_csv(self):
        header = []
        for col in self._data["header"]:
            header.append(col)
            if col in self._data["services"]["LinkR"]["extension"]:
                for col_to_add in self._data["services"]["LinkR"]["extension"][col]:
                    header.append(f"{col_to_add}")

        rows = []
        for id_row, row in enumerate(self._data["cea"]):
            cells = []
            for id_col, col in enumerate(row):
                cells.append(self._data["rows"][id_row]["data"][id_col])
                if self._data["header"][id_col] in self._data["services"]["LinkR"]["extension"]:
                    values = ["Null", "Null", "Null"]
                    for candidate in col:
                        if candidate["match"]:
                            values[0] = candidate["id"]
                            values[1] = candidate["name"]
                            values[2] = candidate["description"]
                    cells.extend(values)
            rows.append(cells)
        return pd.DataFrame(rows, columns=header)


print ("Start decision")    

filename_path = sys.argv[1]
# Reading
with open(filename_path, "rb") as f:
    input_data = orjson.loads(f.read())

decision = Decision(input_data)
cea, candidates = decision.extract_cea_and_candidates_scored_data()
input_data["cea"] = cea
input_data["candidates"] = candidates


decision.get_csv().to_csv("/tmp/output.csv", index=False)

print ("End decision")

# Writing
with open("/tmp/output.json", "wb") as f:
    f.write(orjson.dumps(input_data, option=orjson.OPT_INDENT_2))


print ("End writing")
