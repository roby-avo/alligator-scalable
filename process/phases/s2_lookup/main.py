import os
from lamAPI import LamAPI
from lookup import Lookup
import sys
import json 

SAMPLE_SIZE = 25
LAMAPI_HOST, LAMAPI_PORT = os.environ["LAMAPI_ENDPOINT"].split(":")
LAMAPI_TOKEN = os.environ["LAMAPI_TOKEN"]
lamAPI = LamAPI(LAMAPI_HOST, LAMAPI_PORT, LAMAPI_TOKEN)
filename_path = sys.argv[1]
with open(filename_path) as f:
    input = json.loads(f.read())
    
p1 = Lookup(input, lamAPI)
input["candidates"] = p1._rows

with open(filename_path, "w") as f:
    f.write(json.dumps(input))
