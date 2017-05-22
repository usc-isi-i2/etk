import sys
import os
sys.path.append('../../')
from etk.core import Core
import json
import codecs
import sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score, StratifiedKFold
from sklearn import metrics
from sklearn.metrics import classification_report
import numpy as np

def test_features():
	input_file_path = "1_content_extracted.jl"
	in_file = codecs.open(input_file_path)

	doc = json.loads(in_file.read())
	r = c.process(doc, create_knowledge_graph=True)
	with codecs.open('out_1.jl', 'w') as f:
		f.write(json.dumps(r))

def gen_feature_vectors(input_file_path, country):
	print "Processing country: {}".format(country)
	in_file = codecs.open(input_file_path)

	for index, line in enumerate(in_file):
		print index
		try:
			doc = json.loads(line)
			r = c.process(doc, create_knowledge_graph=True)
			
			for fv in r["knowledge_graph"]["country_classifier"]:
				X_vector.append(json.loads(fv["value"]))
				Y_vector.append(1 if fv["qualifiers"]["country"] == country else 0)
		except Exception:
			pass


def gen_train_data(files, countries):

	for in_file, country in zip(files, countries):
		gen_feature_vectors(in_file, country)
	print len(X_vector)
	print len(Y_vector)


def train_classifier():

	X = np.asarray(X_vector)
	y = np.asarray(Y_vector)

	skf = StratifiedKFold(n_splits=3)

	for train_index, test_index in skf.split(X, y):
		X_train, X_test = X[train_index], X[test_index]
		y_train, y_test = y[train_index], y[test_index]

		clf = LogisticRegression()
		clf.fit(X_train, y_train)
		y_pred = clf.predict(X_test)

		print "Classification Report:"
		print classification_report(y_test, y_pred)


if __name__ == '__main__':
	e_config = json.load(codecs.open('config2.json'))
	c = Core(extraction_config=e_config)
	# test_features()
	path = "/home/vinay/Documents/Study/ISI/nov-eval/country-classifier/"
	files = [path + "groundtruth_france.jl", path + "groundtruth_australia.jl", path + "groundtruth_usa.jl"]
	countries = ["france", "australia", "united states"]
	X_vector = list()
	Y_vector = list()
	gen_train_data(files, countries)
	train_classifier()

