import os
import numpy as np
import pandas as pd
import math
import time
from lamAPI import LamAPI
from lookup import Lookup

SAMPLE_SIZE = 25
LAMAPI_HOST, LAMAPI_PORT = os.environ["LAMAPI_ENDPOINT"].split(":")
LAMAPI_TOKEN = os.environ["LAMAPI_TOKEN"]
lamAPI = LamAPI(LAMAPI_HOST, LAMAPI_PORT, LAMAPI_TOKEN)
p1 = Lookup(output, lamAPI)
output["candidates"] = p1._rows
