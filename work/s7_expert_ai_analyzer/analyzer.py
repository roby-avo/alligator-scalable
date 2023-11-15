import sys
import orjson
import urllib3
import os
import aiohttp
import asyncio
import traceback 
import hashlib
import pandas as pd
import numpy as np

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

headers = {
    "Content-Type": "text/plain",
}

class Analyzer:
    def __init__(self, endpoint, data, columns_to_classify, kg_reference):
        self._endpoint = endpoint
        self._data = data
        self._columns_to_classify = columns_to_classify
        self._kg_reference = kg_reference
        self._lock = asyncio.Lock()  # Initialize a new lock
        self._semaphore = asyncio.Semaphore(5)  # Adjust the number as needed

    
    async def classify_description(self):
        results = []
        for columnn_to_classify in self._columns_to_classify:
            tasks = []
            columns = columnn_to_classify["columns"]
            buffer = self._data[columns[0]].to_numpy()
            for col in columns[1:]:
                buffer += " " + self._data[col].to_numpy()
        
            tasks = buffer.tolist()
            #print(tasks)
            # Check if tasks list is empty, and skip running requests if it is
            if not tasks:
                continue
            
            responses = await self.run_all_requests(tasks)
            results.append(responses)

        return results

    async def send_request(self, session, data):
        key = self._generate_short_fingerprint(data)
        if key in cache:
            return cache[key]
        #print("Send request")
        async with self._semaphore:
            async with session.post(self._endpoint, headers=headers, data=data, ssl=False) as response:
                response_json = await response.json()
                result = response_json["analysis"]["mainTopics"]
                text_result = "\n".join([f"{item['label']}:{item['score']}," for item in result])
                async with self._lock:  # Acquire the lock before accessing the cache
                    # Update the cache while the lock is held
                    cache[self._generate_short_fingerprint(data)] = text_result
      
        return text_result

    async def run_all_requests(self, tasks_data):
        async with aiohttp.ClientSession() as session:
            tasks = [self.send_request(session, text) for text in tasks_data]
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            return responses


    def _generate_short_fingerprint(self, text, length=8):
        # We're using MD5 here for simplicity. For more collision resistance you can use SHA256
        hash_object = hashlib.md5(text.encode('utf-8'))
        # Truncate the hash to the desired length
        return hash_object.hexdigest()[:length]


async def main():    
    print("Start classifier")

    filename_path = sys.argv[1]
    filename_path_csv = sys.argv[2]

    # Reading
    with open(filename_path, "rb") as f:
        input_data = orjson.loads(f.read())
    
    df = pd.read_csv(filename_path_csv)

    ANALYZER_ENDPOINT = os.environ["ANALYZER_ENDPOINT"]

    try:
        analizyer = Analyzer(ANALYZER_ENDPOINT, df, input_data["services"]["StructR"], input_data["kg_reference"])
        responses = await analizyer.classify_description()
        #print(responses)
        header = list(df.columns)
        for i, item in enumerate(input_data["services"]["StructR"]):
            extension = item["extension"]
            for col in extension:
                index = header.index(col)
                df.insert(index+1, extension[col], responses[i], allow_duplicates=True)
        df.to_csv("/tmp/output.csv", index=False)
    except Exception as e:
        print("Error with classifier, details:", str(e), traceback.print_exc())

    print("End classifier")

    # Writing
    with open("/tmp/output.json", "wb") as f:
        f.write(orjson.dumps(input_data, option=orjson.OPT_INDENT_2))


    print("End writing")


with(open("./cache.json", "rb")) as f:
    cache = orjson.loads(f.read())

if __name__ == "__main__":
    asyncio.run(main())
