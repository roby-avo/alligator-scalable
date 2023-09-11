import sys
import json
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

url = "https://deployenv6.expertcustomers.ai:8084/services/emd/analyze"
headers = {
    "Content-Type": "text/plain",
}

class Classifier:
    def __init__(self, data):
        self._data = data

    def classify_description(self):
        for row in self._data["candidates"]:
            for candidates in row:
                if len(candidates) > 0:
                    candidate = candidates[0]
                    temp = candidate["name"] + " " +candidate["description"]
                    temp=temp.encode('utf-8')
                    categories = self._get_categories(temp)
                    candidate["iptc_categories"] = categories["iptc_categories"]
                    candidate["geo_categories"] = categories["geo_categories"]

    def _get_categories(self, data):
        response = requests.post(url, headers=headers, data=data, verify=False)
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
    
    

   
    

filename_path = sys.argv[1]
with open(filename_path) as f:
    input = json.loads(f.read())
    
classifier = Classifier(input)
classifier.classify_description()

with open("/tmp/output.json", "w") as f:
    f.write(json.dumps(input, indent=4))
print(json.dumps(input), flush=True)