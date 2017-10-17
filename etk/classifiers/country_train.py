import sys
import os

sys.path.append('../../')
from etk.core import Core

sys.path.append('../../../dig-extractions-classifier/')
from initClassifiers import ProcessClassifier
import json
import codecs
import sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score, StratifiedKFold
from sklearn import metrics
from sklearn.metrics import classification_report
import numpy as np
import argparse
import urllib2

reload(sys)
sys.setdefaultencoding('utf8')


def test_features():
    input_file_path = "data/1_content_extracted.jl"
    in_file = codecs.open(input_file_path)

    doc = json.loads(in_file.read())
    r = c.process(doc, create_knowledge_graph=True)
    with codecs.open('data/out_1.jl', 'w') as f:
        f.write(json.dumps(r))


def convert_urls_to_cdrs(country):
    print "Crawling data for", country
    in_file = codecs.open('data/{}-ads.txt'.format(country))
    out_file = codecs.open('data/groundtruth_{}.jl'.format(country), 'w', 'utf-8')

    for index, line in enumerate(in_file):
        jl = dict()
        try:
            line = line.strip()
            req = urllib2.Request(line, headers={'User-Agent': "Magic Browser"})
            con = urllib2.urlopen(req)
            html = con.read()
            jl['url'] = line
            jl['raw_content'] = html
            out_file.write(json.dumps(jl) + '\n')
            print "Crawled url", index
        except:
            pass

    out_file.close()
    print "Done crawling", country


def crawl_data(countries):
    for country in countries:
        convert_urls_to_cdrs(country)


def gen_feature_vectors(input_file_path, country):
    X_vector = list()
    Y_vector = list()
    X_data = list()

    embeddings_file = 'unigram-part-00000-v2.json'
    extraction_classifiers = ['city']
    classifier_processor = ProcessClassifier(extraction_classifiers, embeddings_file)
    print "Processing country: {}".format(country)
    in_file = codecs.open(input_file_path)
    for index, line in enumerate(in_file):
        print "Processing jline", index
        try:
            doc = json.loads(line)
            r = c.process(doc, create_knowledge_graph=True)
            result_doc = classifier_processor.classify_extractions(r)

            for fv in result_doc["knowledge_graph"]["country_classifier"]:
                feat_vec = json.loads(fv["value"])
                feat_vec.append(bool_prob_city_in_country(result_doc["knowledge_graph"], country))
                X_vector.append(feat_vec)
                Y_vector.append(1 if fv["qualifiers"]["country"] == country else 0)
                X_data.append(
                    get_doc_repr(result_doc, country, X_vector[-1], Y_vector[-1], fv["qualifiers"]["country"]))
        except Exception:
            pass

    in_file.close()
    return X_vector, Y_vector, X_data


def get_doc_repr(doc, given_country, X_vec, y_vec, country_considering):
    comp = dict()
    comp["url"] = doc["url"]
    comp["country_label"] = given_country
    comp["country_considered"] = country_considering
    comp["X_vector"] = X_vec
    comp["Y_expected"] = y_vec
    comp["city_extractions"] = dict()
    comp["city_extractions"]["content_strict"] = list()
    comp["city_extractions"]["content_relaxed"] = list()

    for city in doc["knowledge_graph"]["city"]:
        if "provenance" in city:
            for prov in city["provenance"]:
                cpr = dict()
                cpr["text"] = prov["source"]["context"]["text"]
                cpr["embedding_probability"] = prov["confidence"]["embedding_probability"]
                segment = prov["source"]["segment"]
                if segment == "content_strict":
                    comp["city_extractions"]["content_strict"].append(cpr)
                else:
                    comp["city_extractions"]["content_relaxed"].append(cpr)

    comp["pop_places"] = list()

    for place in doc["knowledge_graph"]["populated_places"]:
        comp["pop_places"].append(place["key"])

    comp["country_extractions"] = dict()
    comp["country_extractions"]["content_strict"] = list()
    comp["country_extractions"]["content_relaxed"] = list()

    for country in doc["knowledge_graph"]["country"]:
        if "provenance" in country:
            for prov in country["provenance"]:
                if "source" in prov:
                    cpr = dict()
                    cpr["text"] = prov["source"]["context"]["text"]
                    segment = prov["source"]["segment"]
                    if segment == "content_strict":
                        comp["country_extractions"]["content_strict"].append(cpr)
                    else:
                        comp["country_extractions"]["content_relaxed"].append(cpr)

    return comp


def bool_prob_city_in_country(knowledge_graph, given_country):
    """ Returns true if the city with the highest probability
    is the highest populated city in the given country"""

    max_prob = -1
    max_prob_city = None
    cities = knowledge_graph["city"]
    for city in cities:
        for prov in city["provenance"]:
            if prov["confidence"]["embedding_probability"] > max_prob:
                max_prob = prov["confidence"]["embedding_probability"]
                max_prob_city = prov["extracted_value"]

    max_population = -1
    max_population_country = None

    pop_places = knowledge_graph["populated_places"]
    for place in pop_places:
        if place["value"] == max_prob_city:
            if place["qualifiers"]["population"] > max_population:
                max_population = place["qualifiers"]["population"]
                max_population_country = place["qualifiers"]["country"]

    result = 1 if max_population_country == given_country else 0
    return result


def gen_train_data(countries, save=True):
    X_vector = list()
    Y_vector = list()
    X_data = list()

    for country in countries:
        print "Generating feature vectors for", country
        in_file = "data/groundtruth_{}.jl".format(country)
        x_vec, y_vec, x_dat = gen_feature_vectors(in_file, country)
        X_vector.extend(x_vec)
        Y_vector.extend(y_vec)
        X_data.extend(x_dat)

    print "Generating training data, Done!!!"
    print "Length of X:", len(X_vector)
    print "Length of Y:", len(Y_vector)
    print "Length of X_data:", len(X_data)

    X = np.asarray(X_vector)
    y = np.asarray(Y_vector)

    if save:
        print "Saving feature vectors as csv..."
        np.savetxt('feature_vectors.csv', X, delimiter=',')
        print "Saving class labels as csv..."
        np.savetxt('class_labels.csv', y, newline='\n')
        print "Dumping data required for report as json..."
        with codecs.open('data_for_report.jl', 'w') as f:
            f.write(json.dumps(X_data))
        print "Done!"


def load_data():
    X = np.genfromtxt('feature_vectors.csv', delimiter=',')
    y = np.genfromtxt('class_labels.csv', delimiter=',')
    X_data = json.loads(codecs.open('data_for_report.jl').read())

    return X, y, X_data


def train_classifier():
    X = np.genfromtxt('feature_vectors.csv', delimiter=',')
    y = np.genfromtxt('class_labels.csv', delimiter=',')
    X_data = json.loads(codecs.open('data_for_report.jl').read())

    skf = StratifiedKFold(n_splits=3)
    split_index = 1

    for train_index, test_index in skf.split(X, y):
        # print (train_index, test_index)
        X_train, X_test = X[train_index], X[test_index]
        y_train, y_test = y[train_index], y[test_index]

        clf = LogisticRegression()
        # clf = RandomForestClassifier()
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)

        write_report(split_index, test_index, y_pred, X_data)
        print "Classification Report for split", split_index
        print classification_report(y_test, y_pred)
        split_index += 1


def write_report(split_index, test_index, y_pred, X_data):
    out_file = codecs.open('classification_report_split_{}.html'.format(split_index), 'w', 'utf-8')
    out_file.write(u'<!DOCTYPE html><html><head><style> etk {color: red;}</style></head><body>')
    s = u""

    for (i, y) in zip(test_index, y_pred):
        x = X_data[i]
        s = u""
        s += u"<h3>TEST_INDEX: %s" % i
        s += u"</h3><br>URL: %s" % x["url"]
        s += u"<br>Country Label: %s" % x["country_label"]
        s += u"<br>Country considered for calculating features: %s" % x["country_considered"]
        s += u"<br>Feature Vector: %s" % x["X_vector"]
        s += u"<br>Expected Y: %s" % x["Y_expected"]
        s += u"<br>Predicted Y: %s" % y
        if y != x["Y_expected"]:
            s += u"<br>Check this for "
            if y == 1:
                s += u"prec"
            else:
                s += u"recall"
        s += u"<br>City Extractions: "
        s += u"<ol><li>Content Strict: <ul>"
        for city in x["city_extractions"]["content_strict"]:
            s += u"<li>{}, prob: {} </li>".format(city["text"], city["embedding_probability"])
        s += u"</ul></li><li>Content Relaxed: <ul>"
        for city in x["city_extractions"]["content_relaxed"]:
            s += u"<li>{}, prob: {} </li>".format(city["text"], city["embedding_probability"])
        s += u"</ul></li></ol>Populated Places: <ul>"
        for place in x["pop_places"]:
            s += u"<li>{}</li>".format(place)
        s += u"</ul>Country Extractions: "
        s += u"<ol><li>Content Strict: <ul>"
        for country in x["country_extractions"]["content_strict"]:
            s += u"<li>{}</li>".format(country["text"])
        s += u"</ul></li><li>Content Relaxed: <ul>"
        for country in x["country_extractions"]["content_relaxed"]:
            s += u"<li>{}</li>".format(country["text"])
        s += u"</ul></li></ol><br><hr><hr>"
        s += u"####################################################################################"
        out_file.write(s + u"\n")
    out_file.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--features", help="test features on a single doc", action="store_true")
    parser.add_argument("-c", "--crawl", help="crawl data from the urls", action="store_true")
    parser.add_argument("-g", "--generate", help="generate training data from groundtruth", action="store_true")
    parser.add_argument("-t", "--train", help="load data and train a classifier", action="store_true")

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()

    countries = ["france", "australia", "united states"]

    e_config = json.load(codecs.open('config2.json'))
    c = Core(extraction_config=e_config)

    if args.features:
        test_features()
    elif args.crawl:
        crawl_data(countries)
    elif args.generate:
        gen_train_data(countries)
        if args.train:
            train_classifier()
    elif args.train:
        train_classifier()
