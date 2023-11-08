import os
import tensorflow as tf

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

from keras.models import load_model
import sys
import orjson

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
                pred = self._model.predict(column_features, batch_size=10000)
            prediction.append(pred)
            indexes.append(0)
        
        for row in self._data["candidates"]:
            for id_col, candidates in enumerate(row):
                for candidate in candidates:
                    index = indexes[id_col]
                    indexes[id_col] += 1
                    feature = round(float(prediction[id_col][index][1]), 3)
                    if feature_name == "rho2": 
                        candidate[feature_name] = feature
                    else:
                        candidate["features"][feature_name] = feature    
                if feature_name == "rho2":        
                    candidates.sort(key=lambda x:x[feature_name], reverse=True)       
                else:
                    candidates.sort(key=lambda x:x["features"][feature_name], reverse=True)    

print("Start prediction")

filename_path = sys.argv[1]
feature_name = sys.argv[2]

# Reading
with open(filename_path, "rb") as f:
    input_data = orjson.loads(f.read())


model = load_model("neural_network.h5")
Prediction(input_data, model).compute_prediction(feature_name)

print("End prediction")

# Writing
with open("/tmp/output.json", "wb") as f:
    f.write(orjson.dumps(input_data, option=orjson.OPT_INDENT_2))
    
print("End writing")
