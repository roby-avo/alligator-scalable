import os
from lamAPI import LamAPI
import sys
import orjson
import metrics as metrics
import utils as utils


class Lookup:
    def __init__(self, data:object, lamAPI):
        self._header = data.get("header", [])
        self._table_name = data["name"]
        self._target = data["target"]
        self._kg_ref = data["kg_reference"]
        self._limit = data["limit"]
        self._lamAPI = lamAPI
        self._rows = []
        for row in data["rows"]:
            row = self._build_row(row["data"])
            self._rows.append(row)


    def _build_row(self, cells):
        row_candidates = []
        features = ["ntoken", "popularity", "pos_score", "es_score", "es_diff_score", 
                    "ed_score", "jaccard_score", "jaccardNgram_score", "cosine_similarity",
                    "p_subj_ne", "p_subj_lit", "p_obj_ne", "desc", "descNgram", 
                    "cpa", "cpaMax", "cta", "ctaMax", "rho", "diff"]
        cells_to_consider = []
        for i, cell in enumerate(cells):
            if i not in self._target.get("NO_ANN", []):
                cells_to_consider.append(cell)
        row_content_norm = utils.clean_str(" ".join(cells_to_consider))  
        for i, cell in enumerate(cells):
            new_candidites = []
            if i in self._target["NE"]:
                #candidates = self._get_candidates(cell)
                if cell in cache:
                    candidates = cache.get(cell, [])
                else:
                    candidates = self._get_candidates(cell)
                    cache[cell] = candidates    
                    
                for candidate in candidates:
                    item = {
                        "id": candidate["id"],
                        "name": candidate["name"],
                        "description": candidate["description"],
                        "types": candidate["types"],
                        "features": {feature:candidate.get(feature, 0) for feature in features},
                        "matches": {str(id_col):[] for id_col in range(len(cells))},
                        "predicates": {str(id_col):{} for id_col in range(len(cells))},
                        "match": False
                    }
                    new_candidites.append(item)
                    desc_norm = utils.clean_str(candidate["description"])
                    desc_score = round(metrics.compute_similarity_between_string(desc_norm, row_content_norm), 3)
                    desc_score_ngram = round(metrics.compute_similarity_between_string(desc_norm, row_content_norm, 3), 3)
                    item["features"]["desc"] = desc_score
                    item["features"]["descNgram"] = desc_score_ngram
            row_candidates.append(new_candidites)
        return row_candidates


    def _get_candidates(self, cell):
        #print("Try lookup for cell:", cell)
        candidates = []
        types = None
        result = None
        try:
            result = self._lamAPI.lookup(cell, fuzzy=False, types=types, kg=self._kg_ref, limit=self._limit)
            if cell not in result:
                raise Exception("Error from lamAPI")
            candidates = result[cell]    
        except Exception as e:
            print(str(e))
            return []
            
        return candidates


print("Start lookup")

SAMPLE_SIZE = 25
LAMAPI_HOST, LAMAPI_PORT = os.environ["LAMAPI_ENDPOINT"].split(":")
LAMAPI_TOKEN = os.environ["LAMAPI_TOKEN"]
lamAPI = LamAPI(LAMAPI_HOST, LAMAPI_PORT, LAMAPI_TOKEN)
filename_path = sys.argv[1]

# Reading
with open(filename_path, "rb") as f:
    input_data = orjson.loads(f.read())

with(open("./cache.json", "rb")) as f:
    cache = orjson.loads(f.read())

p1 = Lookup(input_data, lamAPI)
input_data["candidates"] = p1._rows

print("End lookup")

# Writing
with open("/tmp/output.json", "wb") as f:
    f.write(orjson.dumps(input_data, option=orjson.OPT_INDENT_2))

# Writing
with open("/tmp/cache.json", "wb") as f:
    f.write(orjson.dumps(cache, option=orjson.OPT_INDENT_2))

print("End writing")
