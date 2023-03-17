from model.row import Row
from phases.entity_linker import EntityLinker
import traceback

class CEAProcess:
    def __init__(self, data:object, lamAPI, rankers: list, target, log_c, types={}, kg_ref="wikidata", size=100):
        self._header = data.get("header", [])
        self._dataset_name = data["datasetName"]
        self._table_name = data["tableName"]
        self._lamAPI = lamAPI
        self._rankers = rankers
        self._target = target
        self._log_c = log_c
        self._kg_ref = kg_ref
        self._size = size
        self._types = types
        self._header_row = self._build_header_row(self._header, 0)
        self._rows = []
        for row in data["rows"]:
            cell_to_size = row.get("cellToSize", {})
            row = self._build_row(row["data"], row["idRow"], cell_to_size)
            self._rows.append(row)


    def compute(self):
        results = []
        candidates = []
        for row in self._rows:
            self._apply_cea_on_row(row, results, candidates)
        return (results, candidates)


    def _apply_cea_on_row(self, row, results, candidates):
        entity_linker = EntityLinker(row, self._lamAPI, self._rankers)
        #entity_linker.compute_similarity_on_candidates_cells()
        entity_linker.compute_relationship_score()
        (winning_candidates, winning_entities, candidates_ranked, candidates_discarded) = entity_linker.compute_final_score()
        results.append({
            "datasetName": self._dataset_name,
            "tableName": self._table_name,
            "row": row._id_row,
            "data": row.get_row_text(),
            "winningCandidates": winning_candidates,
            "cea": winning_entities,
            "kgReference": self._kg_ref
        })

        candidates.append({
            "datasetName": self._dataset_name,
            "tableName": self._table_name,
            "row": row._id_row,
            "candidates": candidates_ranked,
            "candidatesDiscarded": candidates_discarded,
            "kgReference": self._kg_ref
        })


    def _build_row(self, cells, id_row, cell_to_size):
        row = Row(id_row)
        row_text = " ".join([str(cell) for cell in cells])
        for i, cell in enumerate(cells):
            if i in self._target["NE"]:
                candidates = self._get_candidates(cell, i, cell_to_size, id_row)
                is_subject = i == self._target["SUBJ"]
                row.add_ne_cell(cell, row_text, candidates, i, is_subject)
            elif i in self._target["LIT"]:
                row.add_lit_cell(cell, i, self._target["LIT_DATATYPE"][str(i)])
            else:    
                row.add_notag_cell(cell)
        return row


    def _build_header_row(self, cells, id_row):
        row = Row(id_row)
        row_text = " ".join(cells)
        for i, cell in enumerate(cells):
            candidates = self._get_candidates(cell, i, {}, id_row)
            is_subject = i == self._target["SUBJ"]
            row.add_ne_cell(cell, row_text, candidates, i, is_subject)
        return row                  
    

    def _get_candidates(self, cell, id_col, cell_to_size, id_row):
        size = cell_to_size.get(cell, 100)
        candidates = []
        types = None
        if size != 10:
            types = self._types.get(str(id_col))
        cell = " ".join(str(cell).split()).lower()
        params = {
            "cell": cell, 
            "type": types, 
            "kg": self._kg_ref,
            "size": size
        }
        result = None
        try:
            result = self._lamAPI.lookup(cell, ngrams=True, fuzzy=False, types=types, kg=self._kg_ref, limit=size)
            if cell not in result:
                raise Exception("Error from lamAPI")
            candidates = result[cell]    
        except Exception as e:
            self._log_c.insert_one({
                'datasetName': self._dataset_name,
                'tableName': self._table_name,
                'idRow': id_row,
                'cell': cell,
                'params': params,
                'error': str(e), 
                'stackTrace': traceback.format_exc(),
                'result': result
            })
       
        return candidates