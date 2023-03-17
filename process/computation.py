import os
import sys
import time
import traceback

import joblib
import redis
from keras.models import load_model

from process.phases.process.cea import CEAProcess
from phases.cpa import CPAProcess
from phases.cta import CTAProcess
from phases.data_preparation import compute_datatype, pre_processing
from phases.revision import RevisionProcess
from wrapper.lamAPI import LamAPI
from wrapper.logistic_regressor import LogisticRegressor
from wrapper.mongodb_conn import get_collection
from wrapper.neural_network import NeuralNetwork

neural1_path = "./process/ml_models/neural_network1.h5"
neural2_path = "./process/ml_models/neural_network2.h5"
logistic1_path = "./process/ml_models/logistic1.pkl"
logistic2_path = "./process/ml_models/logistic2.pkl"

rankers = [
                NeuralNetwork(load_model(neural1_path), "neural1"), 
                NeuralNetwork(load_model(neural2_path), "neural2"), 
                LogisticRegressor(joblib.load(logistic1_path), "logistic1"),
                LogisticRegressor(joblib.load(logistic2_path), "logistic2")
        ]


start = time.time()

REDIS_ENDPOINT = os.environ["REDIS_ENDPOINT"]
REDIS_JOB_DB = int(os.environ["REDIS_JOB_DB"])
LAMAPI_HOST, LAMAPI_PORT = os.environ["LAMAPI_ENDPOINT"].split(":")
LAMAPI_TOKEN = os.environ["LAMAPI_TOKEN"]

lamAPI = LamAPI(LAMAPI_HOST, LAMAPI_PORT, LAMAPI_TOKEN)
job_active = redis.Redis(host=REDIS_ENDPOINT, db=REDIS_JOB_DB)

row_c = get_collection('row')
log_c = get_collection('log')
cea_init_c = get_collection('ceaInit')
header_cea_c = get_collection('ceaHeader')
cea_c = get_collection('cea')
cpa_c = get_collection('cpa')
cta_c = get_collection('cta')
header_candidate_scored_c = get_collection('headerCandidateScored')
candidate_scored_c = get_collection('candidateScored')
data = row_c.find_one_and_update({"status": "TODO"}, {"$set": {"status": "DOING"}})


if data is None:
    job_active.set("STOP", "")
    sys.exit(0)

header = data.get("header", [])
rows = data["rows"]
kg_reference = data["kgReference"]
candidate_size = data["candidateSize"]
column_metadata = data["column"]
target = data["target"]
type = data["types"]
_id = data["_id"]
dataset_name = data["datasetName"]
table_name = data["tableName"]

types_weights = data["typesWeights"]
predicates_weights = data["predicatesWeights"]
obj_row_update = {"status": "DONE", "time": None}
try:
    if len(column_metadata) == 0:
        column_metadata, target = compute_datatype(rows, lamAPI)
        column_metadata[str(target["SUBJ"])] = "SUBJ"
        obj_row_update["column"] = column_metadata
        obj_row_update["metadata"] = {
            "column": [{"idColumn": int(id_col), "tag": column_metadata[id_col]} for id_col in column_metadata]
        }
        obj_row_update["target"] = target

    cells_set = pre_processing(header, rows, column_metadata, type, candidate_size)
    cea = CEAProcess(data, lamAPI, rankers, target, log_c, type, kg_ref=kg_reference, size=candidate_size)
    (results_cea, candidates) = cea.compute()
    cea_init_c.insert_many(results_cea)
    winning_candidates = [row["winningCandidates"] for row in results_cea]
    keys = {"datasetName":dataset_name, "tableName":table_name}
    cpa = CPAProcess(winning_candidates, keys, target, len(rows[0]["data"]), kg_ref=kg_reference)
    results_cpa = cpa.compute()
    cta = CTAProcess(winning_candidates, keys, target, len(rows[0]["data"]), kg_ref=kg_reference)
    results_cta = cta.compute()
    cpa_c.insert_one(results_cpa)
    cta_c.insert_one(results_cta)
    cpa_data, cta_data = (cpa._cpa, cta._cta)
    if len(types_weights) > 0:
        cta_data = types_weights
    if len(predicates_weights) > 0:
        cpa_data = predicates_weights     
     
    revision = RevisionProcess(rankers, results_cea, candidates, cpa._cpa, cta._cta, target["SUBJ"])
    revision.compute()
    cea_c.insert_many(results_cea)
    candidate_scored_c.insert_many(candidates)
    end = time.time()
    execution_time = round(end - start, 2)
    obj_row_update["time"] = execution_time
    row_c.update_one({"_id": _id}, {"$set": obj_row_update})
except Exception as e:
     log_c.insert_one({
        "datasetName": dataset_name, 
        "tableName": table_name, 
        "error": str(e), 
        "stackTrace": traceback.format_exc()
    })
