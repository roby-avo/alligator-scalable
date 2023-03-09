import utils.metrics as metrics
import utils.utils as utils


class Cell:
    def __init__(self, content: str, row_content:str, candidates: list, id_col: int, is_lit_cell=False, is_notag_cell=False, datatype=None):
        self.content = utils.clean_str(str(content))
        self._id_col = id_col
        self.is_lit_cell = is_lit_cell
        self.is_notag_cell = is_notag_cell
        self.datatype = datatype
        self._candidates = {}
        self._candidates_discarded = {}
       
        for candidate in candidates:
            id_candidate = candidate["id"]
            name_norm = utils.clean_str(candidate["name"]) 
            desc_norm = utils.clean_str(candidate["description"])
            row_content_norm = utils.clean_str(row_content)
            desc_score = round(metrics.compute_similarity_between_string(desc_norm, row_content_norm), 3)
            desc_score_ngram = round(metrics.compute_similarity_between_string(desc_norm, row_content_norm, 3), 3)

            features = {
                "ntoken": candidate["ntoken"],
                "popularity": candidate["popularity"],
                "pos_score": candidate["pos_score"],
                "es_score": candidate["es_score"],
                "es_diff_score": candidate["es_diff_score"],
                "ed": candidate["ed_score"],
                "jaccard": candidate["jaccard_score"],
                "jaccardNgram": candidate["jaccardNgram_score"],
                "cosine_similarity": candidate["cosine_similarity"],
                "p_subj_ne": 0,
                "p_subj_lit": 0,
                "p_obj_ne": 0,
                "desc": desc_score,
                "descNgram": desc_score_ngram,
                "cpa": 0,
                "cpaMax": 0,
                "cta": 0,
                "ctaMax": 0,
                "cea": 0,
                "diff": 0
            }

            if features["ed"] < 0.20:
                self._candidates_discarded[id_candidate] = { 
                    "id": id_candidate,
                    "name": name_norm,
                    "description": desc_norm, 
                    "types": candidate["types"], 
                    "matches": {},
                    "features": features,
                    "pred": {},
                    "match": False,
                    "score": features["ed"] + features["jaccard"]
                }
                continue
            
            replace = False
            if id_candidate in self._candidates:
                score = self._candidates[id_candidate]["features"]["ed"] + self._candidates[id_candidate]["features"]["jaccard"]
                if (features["ed"] + features["jaccard"]) > score:
                    replace = True
            
            if id_candidate not in self._candidates or replace:
                self._candidates[id_candidate] = { 
                    "id": id_candidate,
                    "name": name_norm,
                    "description": desc_norm, 
                    "types": candidate["types"], 
                    "match_count": {"obj": 0, "lit": 0, "rel": 0},
                    "matches": {},
                    "features": features,
                    "pred": {},
                    "match": False,
                    "score": 0
                }
         
           
    def candidates_entities(self):
        return self._candidates


    def candidates_name(self, id_entity):
        return self._candidates.get(id_entity, {}).get("name")

    
    def candidates_description(self, id_entity):
        return self._candidates.get(id_entity, {}).get("description")

    
    def candidates_types(self, id_entity):
        return self._candidates.get(id_entity, {}).get("types")


    def candidates_ed(self, id_entity):
        return self._candidates.get(id_entity, {}).get("ed")    


    def get_id_candidates_entities(self):
        return list(self._candidates.keys())    


    def get_set_id_candidates_entities(self):
        return set(self._candidates.keys())    
        