from dateutil.parser import parse

class LamAPICacheWrapper:
    def __init__(self, objects_dump, literals_dump, types_dump, documents_dump, labels_dump): 
        self._objects_dump = objects_dump
        self._literals_dump = literals_dump
        self._types_dump = types_dump
        self._documents_dump = documents_dump
        self._labels_dump = labels_dump
        
    def objects(self, candidates):
        return self._get_data(candidates, self._objects_dump, "objects")
    
    def literals(self, candidates):
        return self._get_data(candidates, self._literals_dump, "literals")
    
    def types(self, candidates):
        return self._get_data(candidates, self._types_dump, "types")

    def documents(self, candidates):
        candidates = list(candidates)
        results = self._documents_dump.find({"id":{"$in":candidates}})
        data =  {}
        for result in results:
            _id = result["id"]
            data[_id] = {}
            data[_id]["names"] = result["names"][0]
            data[_id]["description"] = result["description"]
            data[_id]["types"] = result["types"]
        return data
       
    def labels(self, candidates):
        candidates = list(candidates)
        results = self._labels_dump.find({"entity":{"$in":candidates}})
        data =  {}
        for result in results:
            _id = result["entity"]
            data[_id] = {}
            data[_id]["labels"] = []
            if result["labels"].get("en"):
                data[_id]["labels"].append(result["labels"].get("en"))
            data[_id]["labels"] += result["aliases"].get("en", [])
        return data
    
    def primary_labels(self, candidates):
        candidates = list(candidates)
        results = self._labels_dump.find({"entity":{"$in":candidates}})
        data =  {}
        for result in results:
            _id = result["entity"]
            data[_id] = {}  
            data[_id] = result["labels"].get("en", "")
        return data
    
    def literal_recognizer(self, column):
        freq_data = {}
        for cell in column:
            cell_datatype = self._get_cell_datatype(cell)
            if cell_datatype not in freq_data:
                freq_data[cell_datatype] = 0
            freq_data[cell_datatype] += 1   
        return freq_data
            
    def _get_data(self, candidates, dump_source, key):
        candidates = list(candidates)
        results = dump_source.find({"entity":{"$in":candidates}})
        data =  {result["entity"]:result[key] for result in results}
        return data
        
    
    def _parse_date(self, str_date):
        date_parsed = None

        try:
            int(str_date)
            str_date = f"{str_date}-01-01"
        except:
            pass   

        try:
            date_parsed = parse(str_date)
        except:
            pass   

        if date_parsed is not None:
            return date_parsed

        try:
            str_date = str_date[1:]
            date_parsed = parse(str_date)
        except:
            pass

        if date_parsed is not None:
            return date_parsed

        try:
            year = str_date.split("-")[0]
            str_date = f"{year}-01-01"
            date_parsed = parse(str_date)
        except:
            pass

        return date_parsed


    def _get_cell_datatype(self, cell):
        try:
            float(cell)
            return "NUMBER"
        except:
            pass

        is_a_date = self._parse_date(cell)
        if is_a_date is not None:
            return "DATETIME"
        if len(cell) > 60:
            return "STRING"
        
        return "ENTITY"