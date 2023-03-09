class CPAProcess:
    def __init__(self, winning_candidates, keys: object, target: object, n_cols, kg_ref="wikidata"):
        self._winning_candidates = winning_candidates
        self._dataset_name = keys["datasetName"]
        self._table_name = keys["tableName"]
        self._target = target
        self._cpa = {str(i):{} for i in range(n_cols)}
        self._kg_ref = kg_ref


    def compute(self):
        for row in self._winning_candidates:
            for id_col in self._target["NE"]:
                column = row[id_col]
                id_col = str(id_col)
                previous_pred = set()
                for candidate in column[0:3]:
                    pred = candidate["pred"]
                    for id_col in pred:
                        id_col = str(id_col)
                        predicates = pred[id_col]
                        for predicate in predicates:
                            if predicate not in previous_pred:
                                previous_pred.add(predicate)
                                if predicate not in self._cpa[id_col]:
                                    self._cpa[id_col][predicate] = 0
                                self._cpa[id_col][predicate] += predicates[predicate] + 1

        winning_predicates = {}
        for id_col in self._cpa:
            tmp = max(self._cpa[id_col].items(), key=lambda x: x[1], default=(None, 0))
            max_pred = tmp[0]
            max_val = tmp[1]
            if max_val == 0:
                continue
            for pred in self._cpa[id_col]:
                self._cpa[id_col][pred] = round(self._cpa[id_col][pred] / max_val, 3) 
            if not max_pred is None:
                winning_predicates[id_col] = max_pred


        results = {
            "datasetName": self._dataset_name,
            "tableName": self._table_name,
            "subjCol": self._target["SUBJ"],
            "winningCandidates": self._cpa,
            "cpa": winning_predicates,
            "kgReference": self._kg_ref
        }

        return results