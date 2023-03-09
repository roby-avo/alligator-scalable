
class CTAProcess:
    def __init__(self, winning_candidates, keys: object, target:object, n_cols, kg_ref="wikidata"):
        self._winning_candidates = winning_candidates
        self._dataset_name = keys["datasetName"]
        self._table_name = keys["tableName"]
        self._target = target
        self._cta = {str(i):{} for i in range(n_cols)}
        self._kg_ref = kg_ref

    def compute(self):
        for row in self._winning_candidates:
            for id_col in self._target["NE"]:
                column = row[id_col]
                id_col = str(id_col)
                previous_type = set()     
                for candidate in column[0:3]:
                    types = candidate["types"]
                    for type in types:
                        id_type = type["id"]
                        if id_type not in previous_type:
                            previous_type.add(id_type)
                            if id_type not in self._cta[id_col]:
                                 self._cta[id_col][id_type] = 0       
                            self._cta[id_col][id_type] += 1

        winning_types = {}
        for id_col in self._cta:
            tmp = max(self._cta[id_col].items(), key=lambda x: x[1], default=(None, 0))
            max_type = tmp[0]
            max_val = tmp[1]
            for type in self._cta[id_col]:
                self._cta[id_col][type] = round(self._cta[id_col][type] / max_val, 3) 
            if not max_type is None:
                winning_types[id_col] = max_type         

        results = {
            "datasetName": self._dataset_name,
            "tableName": self._table_name,
            "winningCandidates": self._cta,
            "cta": winning_types,
            "kgReference": self._kg_ref
        }
        
        return results