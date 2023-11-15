import metrics as metrics
import os
from lamAPI import LamAPI
import sys
import orjson


class FeaturesExtraction:
    def __init__(self, data, lamAPI):
        self._data = data
        self._lamAPI = lamAPI
    
    
    def compute_features(self):
        rows = self._data["rows"]
        target = self._data["target"]
        for index, row in enumerate(rows):
            cells = row["data"]
            for id_col_ne_subj in target["NE"]:
                for id_col_ne_obj in target["NE"]:
                    if id_col_ne_subj == id_col_ne_obj:
                        continue
                    self._compute_similarity_between_ne_cells(index, id_col_ne_subj, id_col_ne_obj)
                for id_col_lit_obj in target["LIT"]:
                    lit_cell_obj = cells[id_col_lit_obj]
                    self._match_lit_cells(index, id_col_ne_subj, id_col_ne_obj, lit_cell_obj, target["LIT_DATATYPE"][str(id_col_lit_obj)])
        self._extract_features()
        
        
    def _extract_features(self):
        features = [[] for id_col in range(len(self._data["metadata"]["column"]))]
        for row in self._data["candidates"]:
            for id_col, candidates in enumerate(row):
                for candidate in candidates:
                    features[id_col].append(list(candidate["features"].values()))
        self._data["features"] = features
    
    def _compute_similarity_between_ne_cells(self, id_row, id_col_subj_cell, id_col_obj_cell):
        subj_candidates = self._data["candidates"][id_row][id_col_subj_cell]
        obj_candidates = self._data["candidates"][id_row][id_col_obj_cell]
        subj_id_candidates = [candidate["id"] for candidate in subj_candidates if candidate["id"] not in cache_obj]
        obj_id_candidates = [candidate["id"] for candidate in obj_candidates]
        
        if len(subj_id_candidates) > 0:
            subjects_objects = self._lamAPI.objects(subj_id_candidates)

        object_rel_score_buffer = {}

        for subj_candidate in subj_candidates:
            id_subject = subj_candidate["id"]
            #subj_candidate_objects = subjects_objects.get(id_subject, {}).get("objects", {})
            #cache_obj[id_subject] = subj_candidate_objects
            if id_subject not in cache_obj:
                subj_candidate_objects = subjects_objects.get(id_subject, {}).get("objects", {})
            else:    
                subj_candidate_objects = cache_obj.get(id_subject, {})
                cache_obj[id_subject] = subj_candidate_objects
            objects_set = set(subj_candidate_objects.keys())
            #subj_candidate["matches"][str(id_col_obj_cell)] = []
            #subj_candidate["pred"][str(id_col_obj_cell)] = {}
              
            objects_itersection = objects_set.intersection(set(obj_id_candidates))
            #print(objects_itersection)
            obj_score_max = 0
            for obj_candidate in obj_candidates:
                id_object = obj_candidate["id"]  
                if id_object not in objects_itersection:
                    continue
                              
                score = obj_candidate["features"]["ed_score"]
                if score > obj_score_max:
                    obj_score_max = score
                   
                if id_object not in object_rel_score_buffer:
                    object_rel_score_buffer[id_object] = 0
                score_rel = subj_candidate["features"]["ed_score"]
                if score_rel > object_rel_score_buffer[id_object]:
                    object_rel_score_buffer[id_object] = score_rel
                for predicate in subj_candidate_objects[id_object]:
                    subj_candidate["matches"][str(id_col_obj_cell)].append({
                        "p": predicate,
                        "o": id_object,
                        "s": round(score, 3)
                    })
                    subj_candidate["predicates"][str(id_col_obj_cell)][predicate] = score
            subj_candidate["features"]["p_subj_ne"] += obj_score_max          
        
        for obj_candidate in obj_candidates:
            id_object = obj_candidate["id"]  
            if id_object not in object_rel_score_buffer:
                continue
            obj_candidate["features"]["p_obj_ne"] += object_rel_score_buffer[id_object]    
        
      
    def _match_lit_cells(self, id_row, id_col_subj_cell, id_col_obj_col, obj_cell, obj_cell_datatype):
    
        def get_score_based_on_datatype(valueInCell, valueFromKG, datatype):
            score = 0
            if datatype == "NUMBER":
                score = metrics.compute_similarty_between_numbers(valueInCell, valueFromKG.lower())
            elif datatype == "DATETIME":
                score = metrics.compute_similarity_between_dates(valueInCell, valueFromKG.lower())
            elif datatype == "STRING":
                score = metrics.compute_similarity_between_string(valueInCell, valueFromKg.lower())
            return score
        
        subj_candidates = self._data["candidates"][id_row][id_col_subj_cell]
        subj_id_candidates = [candidate["id"] for candidate in subj_candidates if candidate["id"] not in cache_lit]
        if len(subj_id_candidates) > 0:
            cand_lamapi_literals = self._lamAPI.literals(subj_id_candidates)
            if len(cand_lamapi_literals) == 0:
                return
        
        datatype = obj_cell_datatype
        
        for subj_candidate in subj_candidates:
            id_subject = subj_candidate["id"]
            #literals = cand_lamapi_literals[id_subject]
            if id_subject not in cache_lit:
                literals = cand_lamapi_literals.get(id_subject, {})
            else:   
                literals = cache_lit.get(id_subject, {})
                cache_lit[id_subject] = literals

            if "literals" in literals:
                literals = literals['literals']    
            #cache_lit[id_subject] = literals    
            if len(literals[datatype]) == 0:
                continue
            #subj_candidate["matches"][str(id_col_obj_col)] = []
            #subj_candidate["pred"][str(id_col_obj_col)] = {}
            #subj_cell.candidates_entities()[subject]["match_count"]["lit"] += 1
            max_score = 0
            for predicate in literals[datatype]:
                for valueFromKg in literals[datatype][predicate]:
                    score = get_score_based_on_datatype(obj_cell, valueFromKg, datatype)
                    score = round(score, 3)
                    if score > 0:
                        subj_candidate["matches"][str(id_col_obj_col)].append({
                            "p": predicate,
                            "o": valueFromKg,
                            "s": round(score, 3)
                        })  
                        if score > max_score:
                            max_score = score
                        if predicate not in subj_candidate["predicates"][str(id_col_obj_col)]:
                            subj_candidate["predicates"][str(id_col_obj_col)][predicate] = 0
                        if score > subj_candidate["predicates"][str(id_col_obj_col)][predicate]:
                            subj_candidate["predicates"][str(id_col_obj_col)][predicate] = score    
                            
            subj_candidate["features"]["p_subj_lit"] += max_score
            subj_candidate["features"]["p_subj_lit"] = round(subj_candidate["features"]["p_subj_lit"], 3)

print("Start features extraction")

LAMAPI_HOST, LAMAPI_PORT = os.environ["LAMAPI_ENDPOINT"].split(":")
LAMAPI_TOKEN = os.environ["LAMAPI_TOKEN"]
lamAPI = LamAPI(LAMAPI_HOST, LAMAPI_PORT, LAMAPI_TOKEN)
filename_path = sys.argv[1]

# Reading
with open(filename_path, "rb") as f:
    input_data = orjson.loads(f.read())

with(open("./cache_obj.json", "rb")) as f:
    cache_obj = orjson.loads(f.read())

with(open("./cache_lit.json", "rb")) as f:
    cache_lit = orjson.loads(f.read())

FeaturesExtraction(input_data, lamAPI).compute_features()

print("Finish features extraction")

# Writing
with open("/tmp/output.json", "wb") as f:
    f.write(orjson.dumps(input_data, option=orjson.OPT_INDENT_2))

# Writing
with open("/tmp/cache_obj.json", "wb") as f:
    f.write(orjson.dumps(cache_obj, option=orjson.OPT_INDENT_2))

# Writing
with open("/tmp/cache_lit.json", "wb") as f:
    f.write(orjson.dumps(cache_lit, option=orjson.OPT_INDENT_2))


print("Finish writing")
