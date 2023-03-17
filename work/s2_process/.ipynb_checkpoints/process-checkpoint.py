import traceback

class Process:
    def __init__(self, data:object, lamAPI):
        self._header = data.get("header", [])
        self._table_name = data["name"]
        self._target = data["target"]
        self._kg_ref = data["kg_reference"]
        self._limit = data["limit"]
        self._lamAPI = lamAPI
        self._rows = []
        for row in data["rows"]:
            row = self._build_row(row["idRow"], row["data"])
            self._rows.append(row)


    def _build_row(self, id_row, cells):
        row_candidates = []
        features = ["ntoken", "popularity", "pos_score", "es_score", "es_diff_score", 
                    "ed_score", "jaccard_score", "jaccardNgram_score", "cosine_similarity",
                    "p_subj_ne", "p_subj_lit", "p_obj_ne", "desc", "descNgram", 
                    "cpa", "cpaMax", "cta", "ctaMax", "cea", "diff"]
        for i, cell in enumerate(cells):
            candidates = []
            if i in self._target["NE"]:
                candidates = self._get_candidates(cell, id_row)
                new_candidites = []
                for candidate in candidates:
                    new_candidites.append({
                        "id": candidate["id"],
                        "name": candidate["name"],
                        "descritpion": candidate["description"],
                        "types": candidate["types"],
                        "features": {feature:candidate.get(feature, 0) for feature in features},
                        "matches": {},
                        "pred": {}
                    })
            row_candidates.append(new_candidites)
        return row_candidates


    def _get_candidates(self, cell, id_row):
        candidates = []
        types = None
        result = None
        try:
            result = self._lamAPI.lookup(cell, ngrams=True, fuzzy=False, types=types, kg=self._kg_ref, limit=self._limit)
            if cell not in result:
                raise Exception("Error from lamAPI")
            candidates = result[cell]    
        except Exception as e:
            print(str(e))
            
        return candidates