import utils.metrics as metrics


class EntityLinker:
    def __init__(self, row, lamAPI, rankers: list):
        self._row = row
        self._lamAPI = lamAPI
        self._rankers = rankers
        
        
    def compute_relationship_score(self):
        ne_cells = self._row.get_ne_cells()
        cells = self._row.get_cells()
        for ne_cell in ne_cells:
            for cell in cells:
                if cell == ne_cell:
                    continue
                elif cell.is_lit_cell:
                    self._match_lit_cells(ne_cell, cell)
                else:
                    self._compute_similarity_between_ne_cells(ne_cell, cell)
                

    def _compute_similarity_between_ne_cells(self, subj_cell, obj_cell):
        subjects_objects = self._lamAPI.objects(list(subj_cell.candidates_entities().keys()))
        object_rel_score_buffer = {}
        for subject, objects in subjects_objects.items():
            if "objects" in objects:
                objects = objects["objects"]
            subj_cell.candidates_entities()[subject]["matches"][str(obj_cell._id_col)] = []
            subj_cell.candidates_entities()[subject]["pred"][str(obj_cell._id_col)] = {}
            objects_set = set(objects.keys())
            obj_score_max = 0
            for candidate in objects_set.intersection(set(obj_cell.candidates_entities().keys())):
                subj_cell.candidates_entities()[subject]["match_count"]["obj"] += 1
                score = obj_cell.candidates_entities()[candidate]["features"]["ed"]
                if score > obj_score_max:
                    obj_score_max = score
                   
                if candidate not in object_rel_score_buffer:
                    object_rel_score_buffer[candidate] = 0
                score_rel = subj_cell.candidates_entities()[subject]["features"]["ed"]
                if score_rel > object_rel_score_buffer[candidate]:
                    object_rel_score_buffer[candidate] = score_rel
                for predicate in objects[candidate]:
                    subj_cell.candidates_entities()[subject]["matches"][str(obj_cell._id_col)].append({
                        "p": predicate,
                        "o": candidate,
                        "s": round(score, 3)
                    })
                    subj_cell.candidates_entities()[subject]["pred"][str(obj_cell._id_col)][predicate] = score
            subj_cell.candidates_entities()[subject]["features"]["p_subj_ne"] += obj_score_max
              
        for candidate in object_rel_score_buffer:
            temp = obj_cell.candidates_entities().get(candidate)
            if temp is not None:
                temp["features"]["p_obj_ne"] += object_rel_score_buffer[candidate]
            
        
    def compute_final_score(self):      

        def check(candidate, max_score, threshold=0.03):
            return (max_score - candidate["score"] < threshold)
        
        winning_candidates = []
        winning_entities = {}
        candidates_rankend = []
        candidates_discarded = []
        #weights = {"ed": 10, "jaccard": 8, "jaccardNgram": 5, "p_subj_ne": 3, "p_subj_lit": 7, "p_obj_ne": 4, "desc":2, "descNgram": 3} 
        cells = self._row.get_cells()
        for id_col, cell in enumerate(cells):
            wc = []
            rank = []
            discarded = []
            if not cell.is_lit_cell:
                candidates = cell.candidates_entities()
                max_score = 0
                features = []
                for candidate in candidates:
                    features_values = {feature: round(candidates[candidate]["features"][feature], 3) for feature in candidates[candidate]["features"]}
                    features.append(list(features_values.values()))
            
                if len(features) > 0:
                    for iteration, ranker in enumerate(self._rankers):
                        predictions = ranker.predict(features)
                        for i, candidate in enumerate(candidates):
                            score = round(float(predictions[i][1]), 3)
                            candidates[candidate][f"{ranker._name}_score"] = score
                            if iteration == 1:
                                candidates[candidate]["score"] = score
                                if score > max_score:
                                    max_score = score      

                rank = sorted(candidates.items(), key=lambda x: x[1]["score"], reverse=True)[0:20]
                wc = [candidate[1] for candidate in rank if check(candidate[1], max_score)]
                discarded = list(cell._candidates_discarded.items())[0:10]
                
                
            winning_candidates.append(wc)
            candidates_rankend.append(rank)
            candidates_discarded.append(discarded)
            if len(wc) == 1:
                wc[0]["match"] = True
                winning_entities[str(id_col)] = wc[0]["id"]     
        return (winning_candidates, winning_entities, candidates_rankend, candidates_discarded)
        

    def _match_lit_cells(self, subj_cell, obj_cell):
    
        def get_score_based_on_datatype(valueInCell, valueFromKG, datatype):
            score = 0
            if datatype == "NUMBER":
                score = metrics.compute_similarty_between_numbers(valueInCell, valueFromKG.lower())
            elif datatype == "DATETIME":
                score = metrics.compute_similarity_between_dates(valueInCell, valueFromKG.lower())
            elif datatype == "STRING":
                score = metrics.compute_similarity_between_string(valueInCell, valueFromKg.lower())
            return score

        cand_lamapi_literals = self._lamAPI.literals(list(subj_cell.candidates_entities().keys()))
        datatype = obj_cell.datatype
        for subject in cand_lamapi_literals:
            literals = cand_lamapi_literals[subject]
            if "literals" in literals:
                literals = literals['literals']
            if len(literals[datatype.lower()]) == 0:
                continue
            subj_cell.candidates_entities()[subject]["matches"][str(obj_cell._id_col)] = []
            subj_cell.candidates_entities()[subject]["pred"][str(obj_cell._id_col)] = {}
            subj_cell.candidates_entities()[subject]["match_count"]["lit"] += 1
            max_score = 0
            for predicate in literals[datatype.lower()]:
                for valueFromKg in literals[datatype.lower()][predicate]:
                    score = get_score_based_on_datatype(obj_cell.content, valueFromKg, datatype)
                    score = round(score, 3)
                    if score > 0:
                        subj_cell.candidates_entities()[subject]["matches"][str(obj_cell._id_col)].append({
                            "p": predicate,
                            "o": valueFromKg,
                            "s": round(score, 3)
                        })  
                        if score > max_score:
                            max_score = score
                        if predicate not in subj_cell.candidates_entities()[subject]["pred"][str(obj_cell._id_col)]:
                            subj_cell.candidates_entities()[subject]["pred"][str(obj_cell._id_col)][predicate] = 0
                        if score > subj_cell.candidates_entities()[subject]["pred"][str(obj_cell._id_col)][predicate]:
                            subj_cell.candidates_entities()[subject]["pred"][str(obj_cell._id_col)][predicate] = score    
            factor = subj_cell.candidates_entities()[subject]["match_count"]["lit"]       
            subj_cell.candidates_entities()[subject]["features"]["p_subj_lit"] += max_score / factor 
            subj_cell.candidates_entities()[subject]["features"]["p_subj_lit"] = round(subj_cell.candidates_entities()[subject]["features"]["p_subj_lit"], 3)
                  