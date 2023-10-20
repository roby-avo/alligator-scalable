from dateutil.parser import parse
import re

def clean_str(s):
    s = s.lower()
    s = re.sub("[\(\[].*?[\)\]]", "", s).strip()
    return " ".join(s.split())


def word2ngrams(text, n=None):
    """ Convert word into character ngrams. """
    if n is None:
        n = len(text)
    return [text[i:i+n] for i in range(len(text)-n+1)]


def get_ngrams(text, n=3):
    ngrams = set()
    for token in text.split(" "):
        temp = word2ngrams(token, n)
        for ngram in temp:
            ngrams.add(ngram)
    return set(ngrams)


def parse_date(str_date):
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


def store_cea_data(store, id_table, rows, final_cea_results):
    docs = []

    for i, row in enumerate(rows):
        docs.append({
            "id_table": id_table,
            "row": row["id_row"],
            "cea": final_cea_results[i]
        })   
    store.insert_many(docs)


def store_cpa_data(store, data): 
    store.insert_one(data)

    
def store_cta_data(store, id_dataset, id_table, table_name, cta_results, winning_types): 
    store.insert_one({
        "idDataset": id_dataset,
        "idTable": id_table,
        "tableName": table_name,
        "winningCandidates": cta_results,
        "cta": winning_types
    })

        
