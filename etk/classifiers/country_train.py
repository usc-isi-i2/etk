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
	out_file = codecs.open("processed_{}.jl".format(country), 'w')
	vectors_file = codecs.open("vectors_{}.jl".format(country), 'w')
	for index, line in enumerate(in_file):
		print index
		try:
			doc = json.loads(line)
			r = c.process(doc, create_knowledge_graph=True)
			out_file.write(json.dumps(r) + '\n')

			for fv in r["knowledge_graph"]["country_classifier"]:
				X_vector.append(json.loads(fv["value"]))
				Y_vector.append(1 if fv["qualifiers"]["country"] == country else 0)
				vectors_file.write('jline: ' + str(index+1) + ', index: ' +  str(len(X_vector)-1) + ' ' + str(X_vector[-1]) + ' ' + str(Y_vector[-1]) + '\n')
		except Exception:
			pass

	in_file.close()
	out_file.close()
	vectors_file.close()


def gen_train_data(files, countries):

	for in_file, country in zip(files, countries):
		gen_feature_vectors(in_file, country)
	print len(X_vector)
	print len(Y_vector)


def train_classifier():

	X = np.asarray(X_vector)
	y = np.asarray(Y_vector)
	f = codecs.open('vectors.jl', 'w')

	skf = StratifiedKFold(n_splits=3)

	for train_index, test_index in skf.split(X, y):
		# print (train_index, test_index)
		X_train, X_test = X[train_index], X[test_index]
		y_train, y_test = y[train_index], y[test_index]

		clf = LogisticRegression()
		# clf = RandomForestClassifier()
		clf.fit(X_train, y_train)
		y_pred = clf.predict(X_test)

		f.write("Trial starts----------------------\n")
		f.write("Index\tExpected\tPredicted\n")
		for (i, a, b) in zip(test_index, y_test, y_pred):
			f.write(str(i) + ' ' + str(a) + ' ' + str(b) + '\n')

		print "Classification Report:"
		print classification_report(y_test, y_pred)

	f.close()

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

