from keras.models import load_model
import sys
import json

class Prediction:
    def __init__(self, data, model):
        self._data = data
        self._model = model
        
    def compute_prediction(self, feature_name):
        prediction = []
        indexes = []
        for column_features in self._data["features"]:
            pred = [] 
            if len(column_features) > 0:
                pred = self._model.predict(column_features)
            prediction.append(pred)
            indexes.append(0)
        
        for row in self._data["candidates"]:
            for id_col, candidates in enumerate(row):
                for candidate in candidates:
                    index = indexes[id_col]
                    indexes[id_col] += 1
                    feature = round(float(prediction[id_col][index][1]), 3)
                    if feature_name == "score": 
                        candidate[feature_name] = feature
                    else:
                        candidate["features"][feature_name] = feature    
                if feature_name == "score":        
                    candidates.sort(key=lambda x:x[feature_name], reverse=True)       
                else:
                    candidates.sort(key=lambda x:x["features"][feature_name], reverse=True)    


filename_path = sys.argv[1]
feature_name = sys.argv[2]

with open(filename_path) as f:
    input = json.loads(f.read())

model = load_model("neural_network.h5")
Prediction(input, model).compute_prediction(feature_name)

with open("/tmp/output.json", "w") as f:
    f.write(json.dumps(input, indent=4))
