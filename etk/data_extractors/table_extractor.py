# -*- coding: utf-8 -*-
from jsonpath_rw import jsonpath, parse
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from bs4 import BeautifulSoup
import json
import re
import pickle

class Toolkit:
    @staticmethod
    def create_table_array(t, put_extractions=False):
        rows = t['rows']
        tt = []
        max_cols = t['features']['max_cols_in_a_row']
        for r in rows:
            new_r = ['' for xx in range(max_cols)]
            for i, c in enumerate(r['cells']):
                text = c['text']
                text = text.lower()
                text = text.strip()
                if put_extractions and 'data_extraction' in c:
                    data_extractions = c['data_extraction']
                    for key in data_extractions.keys():
                        text += ' DUMMY' + key.upper()
                new_r[i] = text.strip()
            tt.append(new_r)
        return tt

    @staticmethod
    def regulize_cells(t):
        for r in t:
            for i in range(len(r)):
                r[i] = re.sub('[0-9]', 'NUM', r[i])
                # for x in re.findall('([0-9])', r[i]):
                #     int_x = int(x)
                #     if int_x < 5:
                #         r[i] = re.sub(x, 'SSSS', r[i])
                #     else:
                #         r[i] = re.sub(x, 'LLLL', r[i])
                for x in re.findall('([a-z][a-z][a-z]+@)', r[i]):
                    r[i] = re.sub(x, 'EMAILNAME ', r[i])

    @staticmethod
    def clean_cells(t): # modifies t
        for r in t:
            for i in range(len(r)):
                r[i] = re.sub(r'[^\x00-\x7F]',' ', r[i]) #remove unicodes
                # r[i] = re.sub('[\'"]', '', r[i]) #remove annoying puncts
                r[i] = re.sub('[^\s\w\.\-\$_%\^&*#~+@"\']', ' ', r[i]) #remove annoying puncts
                for x in re.findall('(\.[a-z])', r[i]):
                    r[i] = re.sub('\.{0}'.format(x[1]), ' {0}'.format(x[1]), r[i])
                r[i] = re.sub('\s+', ' ', r[i])
                r[i] = r[i].strip()

class TableClassification():
    def __init__(self, sem_labels, cl=None):
        self.cl_model = cl
        self.sem_labels = sem_labels
        # self.category_mapping = [{'IN-DOMAIN': 0, 'OUT-DOMAIN': 1},
        #                          {'LAYOUT': 0, 'NOT-LAYOUT': 1},
        #                          {'LAYOUT': 0, 'ENTITY': 1, 'RELATIONAL': 2,
        #                           'MATRIX': 3, 'LIST': 4}]
        self.category_mapping = [['IN-DOMAIN', 'OUT-DOMAIN'],
                                 ['LAYOUT', 'NOT-LAYOUT'],
                                 ['LAYOUT', 'ENTITY', 'RELATIONAL',
                                  'MATRIX', 'LIST']]

    def set_cl_model(self,cl):
        self.cl_model = cl

    def train_cl_model(self, train_tables):
        train_X = []
        train_Y = []
        # labels = ['IN-DOMAIN', 'LAYOUT', []]

        for t in train_tables:
            train_X.append(self.create_feature_vector(t))
            ll = [None]*3
            for i, l in enumerate(t['labels']):
                ll[i] = self.category_mapping[i].index(l)
            train_Y.append(ll)  # labels should be sorted

        train_X = np.matrix(train_X)
        train_Y = np.matrix(train_Y)
        self.cl_model = RandomForestClassifier().fit(train_X, train_Y)
        return self

    def create_feature_vector(self, t):
        struct_feats = t['features']
        sem_feats = self.extract_sem_features(t)

        struct_feats = [xx[1] for xx in sorted(struct_feats.items(), key=lambda x: x[0])]
        sem_feats = [xx[1] for xx in sorted(sem_feats.items(), key=lambda x: x[0])]
        return np.array(struct_feats+sem_feats)

    def extract_sem_features(self, t):
        row_features_aggr = []
        sem_feats = dict()
        num_cols = t['features']['max_cols_in_a_row']
        num_rows = t['features']['no_of_rows']

        # table['features'] = {}
        if num_cols == 0 or num_rows == 0:
            sem_feats['max_sem_type_names_col'] = 0
            sem_feats['max_sem_type_names_row'] = 0
            sem_feats['avg_recognized_value_row'] = 0
            sem_feats['avg_recognized_value_col'] = 0
        else:
            # print(num_cols,num_rows)
            col_features_aggr = [dict() for xx in range(num_cols)]
            for row_i, row in enumerate(t['rows']):
                row_features = dict()
                for i, cell in enumerate(row['cells']):
                    cell_features = self.extract_cell_features(cell)
                    row_features = self.add_arrays(row_features, cell_features)
                    row_features_aggr.append(row_features)
                    col_features_aggr[i] = self.add_arrays(col_features_aggr[i], cell_features)
                    # if cell_features is not None:
                    #     print(cell_features)
                # print('###########')
            # table['features']['row_aggr_features'] = row_features_aggr
            # table['features']['col_aggr_features'] = col_features_aggr
            sem_feats['max_sem_type_names_col'] = max([x['SEMANTIC_TYPE'] for x in col_features_aggr])
            sem_feats['max_sem_type_names_row'] = max([x['SEMANTIC_TYPE'] for x in row_features_aggr])
            sem_feats['avg_recognized_value_row'] = float(sum([len(filter(lambda x: x>0, xx.values())) for xx in row_features_aggr]))/float(num_rows)
            sem_feats['avg_recognized_value_col'] = float(sum([len(filter(lambda x: x>0, xx.values())) for xx in row_features_aggr]))/float(num_cols)
        return sem_feats

    def extract_cell_features(self, cell):
        extractors_suceeded = dict()
        extractors_suceeded['SEMANTIC_TYPE'] = 0
        cell_extraction = parse('data_extraction')
        cell_text_path = parse('text')
        cell_text = [match.value for match in cell_text_path.find(cell)]
        cell_data = [match.value for match in cell_extraction.find(cell)]
        for text in cell_text:
            text = re.sub('[^\w]', ' ', text)
            text = re.sub('\s+', ' ', text)
            if any([x in text for x in self.sem_labels]):
                if 'SEMANTIC_TYPE' not in extractors_suceeded:
                    extractors_suceeded['SEMANTIC_TYPE'] = 1
                else:
                    extractors_suceeded['SEMANTIC_TYPE'] += 1
        for fields in cell_data:
            fields = fields.items()
            for key, val in fields:
                if len(val) != 0:
                    if key not in extractors_suceeded:
                        extractors_suceeded[key] = 1
                    else:
                        extractors_suceeded[key] += 1
        return extractors_suceeded

    def add_arrays(self, a,b):
        res = dict()
        for x in a.keys()+b.keys():
            if x in a and x in b:
                res[x] = a[x] + b[x]
            elif x in a:
                res[x] = a[x]
            else:
                res[x] = b[x]
        return res


    def predict_label(self, t):
        v = self.create_feature_vector(t).reshape(1,-1)
        ll = self.cl_model.predict(v)[0]
        ll = np.array(ll, dtype='int32')
        res = []
        for i, l in enumerate(ll):
            res.append(self.category_mapping[i][l])
        return res

    def predict_label_with_prob(self, t):
        v = self.create_feature_vector(t).reshape(1,-1)
        ll = self.cl_model.predict(v)[0]
        ll = np.array(ll, dtype='int32')
        probs = self.cl_model.predict_proba(v)
        print(probs)
        print(ll)
        res = []
        for i, l in enumerate(ll):
            res.append({'label':self.category_mapping[i][l], 'prob': probs[i][0][l]})
        return res

class InformationExtraction:
    def __init__(self, sem_labels, method='rule_based', model=None):
        self.method = method
        self.sem_labels = sem_labels
        if method == 'rule_based':
            self.sem_label_dict = model

    def determine_sem_type(self, attr_name, attr_val):
        if attr_val == '':
            return None
        if self.method == 'rule_based':
            if attr_name in self.sem_label_dict:
                st = self.sem_label_dict[attr_name]
                if st != 'none':
                    return st
        return None

    def extract_entity(self, t):
        all_res = dict()
        tarr = Toolkit.create_table_array(t)
        Toolkit.clean_cells(tarr)
        num_cols = len(tarr)
        if num_cols == 0:
            return None
        num_rows = len(tarr[0])
        if num_rows != 2:
            return None
        for r in tarr:
            res = dict()
            st = self.determine_sem_type(r[0], r[1])
            if not st:
                continue
            res['value'] = r[1]
            res['context'] = dict(start=0, end=0, input='table_extraction_'+self.method, text='{} | {}'.format(r[0], r[1]))
            res['tarr'] = tarr
            if st in all_res:
                all_res[st].append(res)
            else:
                all_res[st] = [res]
        return all_res

    def extract(self, t):
        if 'labels' not in t:
            return None
        labels = t['labels']
        if 'IN-DOMAIN' not in labels:
            return None
        if 'ENTITY' in labels:
            return self.extract_entity(t)
        else:
            return None

class TableExtraction:
    def __init__(self, classify_tables=False, sem_types=[], cl_model=None):
        self.classify_tables = classify_tables
        if classify_tables:
            self.cl_obj = TableClassification(sem_types, cl_model)

    @staticmethod
    def is_data_cell(cell):
        if cell.table:
            return False
        return True

    @staticmethod
    def is_data_row(row):
        if row.table:
            return False
        cell = row.findAll('th', recursive=False)
        cell.extend(row.findAll('td', recursive=False))
        for td in cell:
            if TableExtraction.is_data_cell(td) == False:
                return False
        return True

    @staticmethod
    def get_data_rows(table):
        data_rows = []
        rows = table.findAll('tr', recursive=False)
        if(table.thead):
            rows.extend(table.thead.findAll('tr', recursive=False))
        if(table.tbody):
            rows.extend(table.tbody.findAll('tr', recursive=False))
        for tr in rows:
            if(TableExtraction.is_data_row(tr)):
                data_rows.append(str(tr))
        return data_rows

    @staticmethod
    def is_data_table(table, k):
        rows = TableExtraction.get_data_rows(table)
        if len(rows) > k:
            return rows
        else:
            return False

    @staticmethod
    def mean(numbers):
        """ Computes mean of a list of numbers """
        return float(sum(numbers)) / max(len(numbers), 1)

    @staticmethod
    def _ss(data):
        """Return sum of square deviations of sequence data."""
        c = TableExtraction.mean(data)
        ss = sum((x-c)**2 for x in data)
        return ss

    @staticmethod
    def pstdev(data):
        """Calculates the population standard deviation."""
        n = len(data)
        if n < 2:
            return 0
            # raise ValueError('variance requires at least two data points')
        ss = TableExtraction._ss(data)
        pvar = ss/n # the population variance
        return pvar**0.5

    @staticmethod
    # Check if a string contains a digit
    def contains_digits(d):
        _digits = re.compile('\d')
        return bool(_digits.search(d))

    @staticmethod
    def gen_context(seq):
        seen = set()
        seen_add = seen.add
        uniq_list = [x for x in seq if not (x in seen or seen_add(x))]
        if len(uniq_list) > 5:
            uniq_list = uniq_list[:5]
        uniq_list = [x.replace("\t", "").replace("\r", "").replace("\n", "").strip() for x in uniq_list]
        if '' in uniq_list:
            uniq_list.remove('')
        if ' ' in uniq_list:
            uniq_list.remove(' ')
        return uniq_list

    def extract(self, html_doc, min_data_rows = 1):
        soup = BeautifulSoup(html_doc, 'html.parser')

        if(soup.table == None):
            return None
        else:
            result_tables = list()
            tables = soup.findAll('table')
            for table in tables:
                tdcount = 0
                max_tdcount = 0
                img_count = 0
                href_count = 0
                inp_count = 0
                sel_count = 0
                colspan_count = 0
                colon_count = 0
                len_row = 0
                table_data = ""
                data_table = dict()
                row_list = list()
                rows = TableExtraction.is_data_table(table, min_data_rows)
                if(rows != False):
                    features = dict()
                    row_len_list = list()
                    avg_cell_len = 0
                    avg_row_len_dev = 0
                    for index_row, row in enumerate(rows):
                        row_dict = dict()
                        soup_row = BeautifulSoup(row, 'html.parser')
                        row_data = ''.join(soup_row.stripped_strings)
                        row_data = row_data.replace("\\t", "").replace("\\r", "").replace("\\n", "")
                        if row_data != '':
                            row_len_list.append(len(row_data))
                            row_tdcount = len(soup_row.findAll('td')) + len(soup_row.findAll('th'))
                            if(row_tdcount > max_tdcount):
                                max_tdcount = row_tdcount
                            tdcount += row_tdcount
                            img_count += len(soup_row.findAll('img'))
                            href_count += len(soup_row.findAll('a'))
                            inp_count += len(soup_row.findAll('input'))
                            sel_count += len(soup_row.findAll('select'))
                            colspan_count += row_data.count("colspan")
                            colon_count += row_data.count(":")
                            len_row += 1
                            table_data += row
                            # row_dict["row"] = str(row)
                            cell_list = list()
                            for index_col, td in enumerate(soup_row.findAll('th')):
                                cell_dict = dict()
                                cell_dict["cell"] = str(td)
                                # cell_dict["text"] = [{"result": {"value": ''.join(td.stripped_strings)}}]
                                cell_dict["text"] = ''.join(td.stripped_strings)
                                # cell_dict["id"] = 'row_{0}_col_{1}'.format(index_row, index_col)
                                avg_cell_len += len(cell_dict["text"])
                                cell_list.append(cell_dict)
                            for index_col, td in enumerate(soup_row.findAll('td')):
                                cell_dict = dict()
                                cell_dict["cell"] = str(td)
                                # cell_dict["text"] = [{"result": {"value": ''.join(td.stripped_strings)}}]
                                cell_dict["text"] = ''.join(td.stripped_strings)
                                # cell_dict["id"] = 'row_{0}_col_{1}'.format(index_row, index_col)
                                avg_cell_len += len(cell_dict["text"])
                                cell_list.append(cell_dict)
                            avg_row_len_dev += TableExtraction.pstdev([len(x["text"]) for x in cell_list])
                            row_dict["cells"] = cell_list
                            row_list.append(row_dict)

                    # To avoid division by zero
                    if len_row == 0:
                        tdcount = 1
                    features["no_of_rows"] = len_row
                    features["no_of_cells"] = tdcount
                    features["max_cols_in_a_row"] = max_tdcount
                    features["ratio_of_img_tags_to_cells"] = img_count*1.0/tdcount
                    features["ratio_of_href_tags_to_cells"] = href_count*1.0/tdcount
                    features["ratio_of_input_tags_to_cells"] = inp_count*1.0/tdcount
                    features["ratio_of_select_tags_to_cells"] = sel_count*1.0/tdcount
                    features["ratio_of_colspan_tags_to_cells"] = colspan_count*1.0/tdcount
                    features["ratio_of_colons_to_cells"] = colon_count*1.0/tdcount
                    features["avg_cell_len"] = avg_cell_len*1.0/tdcount
                    features["avg_row_len"] = TableExtraction.mean(row_len_list)
                    features["avg_row_len_dev"] = avg_row_len_dev*1.0/max(len_row, 1)

                    avg_col_len = 0
                    avg_col_len_dev = 0
                    no_of_cols_containing_num = 0
                    no_of_cols_empty = 0


                    if(colspan_count == 0.0 and len_row != 0 and (tdcount/(len_row * 1.0)) == max_tdcount):
                        col_data = dict()
                        for i in range(max_tdcount):
                            col_data['c_{0}'.format(i)] = []
                        soup_col = BeautifulSoup(table_data, 'html.parser')
                        for row in soup_col.findAll('tr'):
                            h_index = 0
                            h_bool = True
                            for col in row.findAll('th'):
                                col_content = ''.join(col.stripped_strings)
                                h_bool = False
                                if col_content is None:
                                    continue
                                else:
                                    col_data['c_{0}'.format(h_index)].append(col_content)
                                h_index += 1
                            d_index = 0
                            if(h_index == 1 and h_bool == False):
                                d_index = 1
                            for col in row.findAll('td'):
                                col_content = ''.join(col.stripped_strings)
                                if col_content is None:
                                    d_index += 1
                                    continue
                                else:
                                    col_data['c_{0}'.format(d_index)].append(col_content)
                                d_index += 1

                        for key, value in col_data.iteritems():
                            whole_col = ''.join(value)
                            # avg_cell_len += float("%.2f" % mean([len(x) for x in value]))
                            avg_col_len += sum([len(x) for x in value])
                            avg_col_len_dev += TableExtraction.pstdev([len(x) for x in value])
                            no_of_cols_containing_num += 1 if TableExtraction.contains_digits(whole_col) is True else 0
                            # features["column_" + str(key) + "_is_only_num"] = whole_col.isdigit()
                            no_of_cols_empty += 1 if (whole_col == '') is True else 0
                    # To avoid division by zero
                    if max_tdcount == 0:
                        max_tdcount = 1
                    features["avg_col_len"] = avg_col_len*1.0/max_tdcount
                    features["avg_col_len_dev"] = avg_col_len_dev/max_tdcount
                    features["no_of_cols_containing_num"] = no_of_cols_containing_num
                    features["no_of_cols_empty"] = no_of_cols_empty
                    data_table["features"] = features
                    data_table["rows"] = row_list
                    context_before = ' '.join(TableExtraction.gen_context(table.find_all_previous(string=True)))
                    context_after = ' '.join(TableExtraction.gen_context(table.find_all_next(string=True)))
                    table_rep = TableExtraction.gen_html(row_list)
                    fingerprint = TableExtraction.create_fingerprint(table_rep)
                    data_table["context_before"] = context_before
                    data_table["context_after"] = context_after
                    data_table["fingerprint"] = fingerprint
                    data_table['html'] = table_rep
                    if self.classify_tables:
                        self.put_classification(data_table)
                    result_tables.append(data_table)
            return result_tables

    def put_classification(self, data_table):
        if self.cl_obj:
            data_table['labels'] = self.cl_obj.predict_label(data_table)

    @staticmethod
    def create_fingerprint(table):
        table = str(table)
        all_tokens = list(set(re.split('[^\w]+',table)))
        all_tokens = sorted(all_tokens)
        fingerprint = '-'.join(all_tokens)
        return fingerprint

    @staticmethod
    def gen_html(row_list):
        """ Return html table string from a list of data rows """
        table = "<table>"
        for row in row_list:
            table += "<tr>"
            cells = row["cells"]
            for cell in cells:
                table += str(cell["cell"])
            table += "</tr>"
        table += "</table>"
        return table

    @staticmethod
    def remove_tables(html_doc, min_data_rows = 1):
        soup = BeautifulSoup(html_doc, 'html.parser')
        tables = soup.findAll('table')
        for table in tables:
            rows = TableExtraction.is_data_table(table, min_data_rows)
            if(rows != False):
                table.decompose()

        return soup

if __name__ == '__main__':
    annotated_file = open('/Users/majid/DIG/dig-table-extractor/experiments/data/50-pages-groundtruth-final2.jl')
    sem_labels_file = open('/Users/majid/DIG/dig-table-extractor/experiments/data/HT-attribute-labels.json')
    train_tables = []
    for line in annotated_file:
        t = json.loads(line)
        if 'THROW' not in t['labels']:
            train_tables.append(t)
    if False:
        tc = TableClassification(json.load(sem_labels_file))
        tc.train_cl_model(train_tables)
        pickle.dump(tc.cl_model, open('/Users/majid/DIG/dig-table-extractor/experiments/data/table_cl_model.bin','wb'))
    else:
        cl_model = pickle.load(open('/Users/majid/DIG/dig-table-extractor/experiments/data/table_cl_model.bin','rb'))
        tc = TableClassification(json.load(sem_labels_file), cl_model)
    print(tc.cl_model.classes_)
    print(tc.create_feature_vector(train_tables[0]))
    print tc.predict_label(train_tables[0])
    # tc.train_cl_model(train_tables)
    # print tc.predict_label(train_tables[0])
    # pickle.dump(tc.cl_model, open('/Users/majid/DIG/dig-table-extractor/experiments/data/table_cl_model.bin','wb'))





