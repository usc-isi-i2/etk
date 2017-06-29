import pickle
import numpy as np


model=pickle.load(open('/Users/amandeep/Github/etk/etk/resources/country_classifier_random_forest', 'r'))
# print model


vector = [0, 0, 0, 0, 0, 1, 0]
v = np.array(vector).reshape(1,-1)

print model.predict_proba(v)[0][1]