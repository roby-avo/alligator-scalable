import os
from pymongo import MongoClient
from wrapper.lamapi_cache import LamAPICacheWrapper


LAMAPI_DB, LAMAPI_DB_PORT = os.environ["LAMAPI_DB"].split(":")
LAMAPI_USERNAME = "admin"
LAMAPI_PW = os.environ["LAMAPI_PW"]


def get_lamapi_connector(kg_reference):
    mongo_lamapi = MongoClient(LAMAPI_DB, int(LAMAPI_DB_PORT), 
                                username=LAMAPI_USERNAME, password=LAMAPI_PW)
    objects_dump = mongo_lamapi[kg_reference].objects
    literals_dump = mongo_lamapi[kg_reference].literals
    types_dump = mongo_lamapi[kg_reference].types
    documents_dump = mongo_lamapi[kg_reference].documents
    labels_dump = mongo_lamapi[kg_reference].labels
    lcw = LamAPICacheWrapper(objects_dump, literals_dump, types_dump, documents_dump, labels_dump)
    return lcw
