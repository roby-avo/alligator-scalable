import sys
import json

THRESHOLD = 0.03

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
                for candidate in candidates:
                    if (candidates[0]["score"] - candidate["score"]) < THRESHOLD:
                        wc.append(candidate)
                
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