import sys
import json

K = 0.6

class Export:
    def __init__(self, data):
        self._data = data


    def extract_cea_and_candidates_scored_data(self):
        cea_data = []
        candidates_scored_data = []
    
        for row in self._data["candidates"]:
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

                winning_candidates.append(wc)
                rankend_candidates.append(rank)

            cea_data.append(winning_candidates)

            candidates_scored_data.append(rankend_candidates)

        return cea_data, candidates_scored_data

   
    

filename_path = sys.argv[1]
with open(filename_path) as f:
    input = json.loads(f.read())

export = Export(input)
cea, candidates = export.extract_cea_and_candidates_scored_data()
input["cea"] = cea
input["candidates"] = candidates

with open("/tmp/output.json", "w") as f:
    f.write(json.dumps(input, indent=4))
print(json.dumps(input), flush=True)