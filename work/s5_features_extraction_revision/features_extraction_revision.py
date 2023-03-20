import sys
import json

class FeaturesExtractionRevision:
    def __init__(self, data):
        self._data = data
        self._cta = {str(id_col):{} for id_col in range(len(self._data["metadata"]["column"]))}
        self._cpa = {str(id_col):{} for id_col in range(len(self._data["metadata"]["column"]))}
        self._compute_cta_and_cpa_freq()
        
        
    def compute_features(self):
        features = [[] for id_col in range(len(self._data["metadata"]["column"]))]
        for id_row, row in enumerate(self._data["candidates"]):
            for id_col, candidates in enumerate(row):
                id_col = str(id_col)
                for candidate in candidates:
                    (cta, ctaMax) = (0, 0)
                    for t in candidate["types"]:
                        if t["id"] in self._cta[id_col]:
                            cta += self._cta[id_col][t["id"]]
                            if self._cta[id_col][t["id"]] > ctaMax:
                                ctaMax = self._cta[id_col][t["id"]]
                    
                    (cpa, cpaMax) = (0, 0)
                    total_predicates = 0
                    for id_col_pred in candidate["predicates"]:
                        for id_predicate in candidate["predicates"][id_col_pred]:
                            if id_predicate in self._cpa[id_col]:
                                total_predicates += 1
                                cpa += self._cpa[id_col][id_predicate]
                                if self._cpa[id_col][id_predicate] > cpaMax:
                                    cpaMax = self._cpa[id_col][id_predicate]
                                
                    cta /= len(candidate["types"]) if len(candidate["types"]) > 0 else 1
                    candidate["features"]["cta"] = round(cta, 2)
                    candidate["features"]["ctaMax"] = ctaMax
                    
                    cpa /= total_predicates if total_predicates > 0 else 1
                    candidate["features"]["cpa"] = round(cpa, 2)
                    candidate["features"]["cpaMax"] = cpaMax
                    
                    candidate["features"]["diff"] = candidates[0]["features"]["cea"] - candidate["features"]["cea"]
                    
                    features[int(id_col)].append(list(candidate["features"].values()))
        self._data["features"] = features          
                    
            
    def _compute_cta_and_cpa_freq(self):
        for row in self._data["candidates"]:
            for id_col, candidates in enumerate(row):
                id_col = str(id_col)
                history = set()
                for candidate in candidates[0:3]:
                    types = candidate["types"]
                    for t in types:
                        id_type = t["id"]
                        if id_type in history:
                            continue
                        if id_type not in self._cta[id_col]:
                            self._cta[id_col][id_type] = 0
                        self._cta[id_col][id_type] += 1
                        history.add(id_type)
                    
                    predicates = candidate["predicates"]
                    for id_col_rel in predicates:
                        for id_predicate in predicates[id_col_rel]:
                            if id_predicate in history:
                                continue
                            if id_predicate not in self._cpa[id_col]:
                                self._cpa[id_col][id_predicate] = 0
                            self._cpa[id_col][id_predicate] += 1
                            history.add(id_predicate)
        
        n_rows = len(self._data["rows"])
        for id_col in self._cta:
            for id_type in self._cta[id_col]:
                self._cta[id_col][id_type] = round(self._cta[id_col][id_type]/n_rows, 2)
            for id_predicate in self._cpa[id_col]:
                self._cpa[id_col][id_predicate] = round(self._cpa[id_col][id_predicate]/n_rows, 2)    


filename_path = sys.argv[1]
with open(filename_path) as f:
    input = json.loads(f.read())

fe_revision = FeaturesExtractionRevision(input)
fe_revision.compute_features()
input["cta"] = fe_revision._cta
input["cpa"] = fe_revision._cta

with open("/tmp/output.json", "w") as f:
    f.write(json.dumps(input, indent=4))
