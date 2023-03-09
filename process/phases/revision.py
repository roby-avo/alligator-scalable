
class RevisionProcess:
    def __init__(self, rankers: list,  cea_results, candidates, cpa, cta, idx_subj=0):
        self._rankers = rankers
        self._cea_results = cea_results
        self._candidates = candidates
        self._cpa = cpa
        self._cta = cta
        self._idx_subj = idx_subj


    def compute(self):

        def check(candidate, max_score, threshold=0.03):
            return (max_score - candidate["score"] < threshold)


        #weights = {"cpa": 2, "cta": 3, "ctaMax": 2, "cpaMax": 1} 
        for id_row, row in enumerate(self._cea_results):
            rankend_candidates = self._candidates[id_row]["candidates"]
            row["confidence"] = 0
            for id_col, candidates_col in enumerate(rankend_candidates):
                id_col = str(id_col)
                max_final_score = 0
                features = {ranker._name:[] for ranker in self._rankers}
                for candidate in candidates_col:
                    total_predicates = set()
                    candidate = candidate[1]
                    predicates = candidate["pred"]
                    cpa_score = 0
                    max_cpa_score = 0
                    for idCol in predicates:
                        wp = max(predicates[idCol].items(), key=lambda x: x[1], default=[None])[0]
                        if wp is not None:
                            total_predicates.add(wp)
                        cpa_score += self._cpa.get(idCol, {}).get(wp, 0)
                        if self._cpa.get(idCol, {}).get(wp, 0) > max_cpa_score:
                            max_cpa_score = self._cpa.get(idCol, {}).get(wp, 0)
                   
                    types = candidate["types"]         
                    cta_score = 0
                    max_cta_score = 0
                    for t in types[0:5]:
                        id_type = t["id"]
                        cta_score += self._cta.get(id_col, {}).get(id_type, 0)
                        if self._cta.get(id_col, {}).get(id_type, 0) > max_cta_score:
                            max_cta_score = self._cta.get(id_col, {}).get(id_type, 0)
                    
                    
                    total_predicates = len(total_predicates) if len(total_predicates) > 0 else 1
                    total_predicates = 1
                    candidate["features"]["cpa"] = round(cpa_score/total_predicates, 3)
                    candidate["features"]["cpaMax"] = max_cpa_score
                    total_types = len(types) if len(types) > 0 else 1
                    total_types = 1
                    candidate["features"]["cta"] = round(cta_score/total_types, 3)
                    candidate["features"]["ctaMax"] = max_cta_score
                    
                    for name in features:
                        score = candidate[f"{name}_score"]
                        candidate["features"]["cea"] = score
                        candidate["features"]["diff"] =  candidates_col[0][1][f"{name}_score"] - score
                        features[name].append(list(candidate["features"].values()))
                


                max_final_score = 0
                for iteration, ranker in enumerate(self._rankers): 
                    if len(features[ranker._name]) > 0:
                        predictions = ranker.predict(features[ranker._name])
                        for i, candidate in enumerate(candidates_col):
                            candidate = candidate[1]
                            score = round(float(predictions[i][1]), 3)
                            candidate[f"{ranker._name}_score"] = score
                            if iteration == 1:
                                candidate["score"] = score
                                if score > max_final_score:
                                    max_final_score = score 


                rank = sorted(candidates_col, key=lambda x: x[1]["score"], reverse=True)
                wc = [candidate[1] for candidate in rank if check(candidate[1], max_final_score)]
                if len(wc) == 1:
                    wc[0]["match"] = True
                    row["cea"][id_col] = wc[0]["id"]
                    row["confidence"] +=  wc[0]["score"]
                row["winningCandidates"][int(id_col)] = wc
                self._candidates[id_row]["candidates"][int(id_col)] = rank
