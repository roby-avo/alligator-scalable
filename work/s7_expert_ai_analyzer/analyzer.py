import sys
import orjson
import urllib3
import os
import aiohttp
import asyncio
import traceback 
import hashlib

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

headers = {
    "Content-Type": "text/plain",
}

class Classifier:
    def __init__(self, endpoint, data, columns_to_classify):
        self._endpoint = endpoint
        self._data = data
        self._columns_to_classify = columns_to_classify
        self._lock = asyncio.Lock()  # Initialize a new lock

    async def classify_description(self):
        tasks = []
        for id_row, row in enumerate(self._data["cea"]):
            for columnn_to_classify in self._columns_to_classify:
                target = columnn_to_classify["extension"]
                columns = columnn_to_classify["columns"]
                id_col = self._data["header"].index(target)
                candidates = row[id_col]
                if len(candidates) == 0:
                    continue
                candidate = candidates[0]        
                
                if columns == ["name", "description"]:  
                    text = candidate["name"] + " " + candidate["description"]
                else:
                    indexes = [self._data["header"].index(col) for col in columns]
                    text = " ".join([self._data["rows"][id_row]["data"][index] for index in indexes])
                text = text.encode('utf-8')
                tasks.append((text, candidate, columns))
                
        # Check if tasks list is empty, and skip running requests if it is
        if not tasks:
            return []
        
        responses = await self.run_all_requests(tasks)
        return responses

    async def send_request(self, session, candidate, columns, data):
        key = " ".join([candidate["id"]] + columns + [self._generate_short_fingerprint(str(data))])
        result = cache.get(key)
        if result is None:
            async with session.post(self._endpoint, headers=headers, data=data, ssl=False) as response:
                response_json = await response.json()
                async with self._lock:  # Acquire the lock before accessing the cache
                    # Update the cache while the lock is held
                    cache[key] = response_json["analysis"]["mainTopics"]
                candidate["_".join(columns)+"_standard_taxonomy"] = [f"{topic['label']} {topic['score']}" for topic in response_json["analysis"]["mainTopics"]]
        else:
            candidate["_".join(columns)+"_standard_taxonomy"] = result
        return    

    async def run_all_requests(self, tasks_data):
        async with aiohttp.ClientSession() as session:
            tasks = [self.send_request(session, candidate, columns, text) for text, candidate, columns in tasks_data]
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
    # Reading
    with open(filename_path, "rb") as f:
        input_data = orjson.loads(f.read())

    ANALYZER_ENDPOINT = os.environ["ANALYZER_ENDPOINT"]

    try:
        classifier = Classifier(ANALYZER_ENDPOINT, input_data, input_data["services"]["StructR"])
        responses = await classifier.classify_description()
        print(responses)
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
