from model.cell import Cell

class Row:
    def __init__(self, id_row):
        self._id_row = id_row
        self.subject_cell = None
        self.cells = []

    def add_ne_cell(self, content: str, row_content: str, candidates: list, id_col: int, is_subject=False):
        cell = Cell(content, row_content, candidates, id_col)
        self.cells.append(cell)

        if is_subject:
            self.subject_cell = cell

    def add_lit_cell(self, content: str,  id_col: int, datatype: str):
        cell = Cell(content, "", [], id_col, True, datatype=datatype)
        self.cells.append(cell)
    
    def add_notag_cell(self, content: str, id_col: int):
        cell = Cell(content, "", [], id_col, is_notag_cell=True)
        self.cells.append(cell)

    def get_subject_cell(self):
        return self.subject_cell

    def get_ne_cells(self):
        ne_cells = []
        for cell in self.cells:
            if not cell.is_lit_cell:
                ne_cells.append(cell)
        return ne_cells        

    def get_columns_type(self):
        types = {"SUBJ": None, "NE": [], "LIT": []}
        for cell in self.cells:
            if cell == self.subject_cell:
                types["SUBJ"] = cell._id_col
            elif cell.is_lit_cell:    
                types["LIT"].append(cell._id_col)
            else:    
                types["NE"].append(cell._id_col)    
        return types

    def get_cells(self):
        return self.cells

    def get_row_text(self):    
        buffer = []
        for cell in self.cells:
            buffer.append(cell.content)
        return buffer    

    def get_text(self):    
        buffer = []
        for cell in self.cells:
            buffer.append(cell.content)
        return " ".join(buffer)

    def __len__(self):
        return len(self.cells)