import sys
import orjson
import requests
import urllib3
import os
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

headers = {
    "Content-Type": "text/plain",
}

class Classifier:
    def __init__(self, endpoint, data, column_to_classify):
        self._endpoint = endpoint
        self._data = data

    def classify_description(self):
        for row in self._data["candidates"]:
            for candidates in row:
                if len(candidates) > 0:
                    candidate = candidates[0]
                    if candidate["id"] not in cache:
                        temp = candidate["name"] + " " + candidate["description"]
                        temp = temp.encode('utf-8')
                        categories = self._get_categories(temp)["iptc_categories"]
                    else:
                        categories = cache.get(candidate["id"], [])
                    candidate["categories"] = categories
                    #candidate["iptc_categories"] = categories["iptc_categories"]
                    #candidate["geo_categories"] = categories["geo_categories"]

    def _get_categories(self, data):
        response = requests.post(self._endpoint, headers=headers, data=data, verify=False)
        result = {"iptc_categories": [], "geo_categories": []}
        if response.status_code == 200:
            print("Request was successful")
            print("Response JSON:")
            print(response.json())
            result = response.json()
            result = {"iptc_categories":result["iptc_categories"], "geo_categories":result["geo_categories"]}
        else:
            print(f"Failed to retrieve data. HTTP Status code: {response.status_code}")
        return result
    
    

   
print("Start classifier")

filename_path = sys.argv[1]
# Reading
with open(filename_path, "rb") as f:
    input_data = orjson.loads(f.read())

with(open("./cache.json", "rb")) as f:
    cache = orjson.loads(f.read())

CLASSIFIER_ENDPOINT = os.environ["CLASSIFIER_ENDPOINT"]

try:
    classifier = Classifier(CLASSIFIER_ENDPOINT, input_data)
    classifier.classify_description()
except Exception as e:
    print("Error with classifier, details:", str(e))

print("End classifier")

# Writing
with open("/tmp/output.json", "wb") as f:
    f.write(orjson.dumps(input_data, option=orjson.OPT_INDENT_2))

print("End writing")
