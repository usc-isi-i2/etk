# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import re

def is_data_cell(cell):
    if(cell.table):
        return False
    return True

def is_data_row(row):
    if(row.table):
        return False
    cell = row.findAll('th', recursive=False)
    cell.extend(row.findAll('td', recursive=False))
    for td in cell:
        if(is_data_cell(td) == False):
            return False
    return True

def get_data_rows(table):
    data_rows = []
    rows = table.findAll('tr', recursive=False)
    if(table.thead):
        rows.extend(table.thead.findAll('tr', recursive=False))
    if(table.tbody):
        rows.extend(table.tbody.findAll('tr', recursive=False))
    for tr in rows:
        if(is_data_row(tr)):
            data_rows.append(str(tr))
    return data_rows

def is_data_table(table, k):
    rows = get_data_rows(table)
    if(len(rows) > k):
        return rows
    else:
        return False


def mean(numbers):
    """ Computes mean of a list of numbers """
    return float(sum(numbers)) / max(len(numbers), 1)

def _ss(data):
    """Return sum of square deviations of sequence data."""
    c = mean(data)
    ss = sum((x-c)**2 for x in data)
    return ss

def pstdev(data):
    """Calculates the population standard deviation."""
    n = len(data)
    if n < 2:
        return 0
        # raise ValueError('variance requires at least two data points')
    ss = _ss(data)
    pvar = ss/n # the population variance
    return pvar**0.5

# Check if a string contains a digit
_digits = re.compile('\d')
def contains_digits(d):
    return bool(_digits.search(d))

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

def extract(html_doc, min_data_rows = 1):
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
            rows = is_data_table(table, min_data_rows)
            if(rows != False):
                features = dict()
                row_len_list = list()
                avg_cell_len = 0
                avg_row_len_dev = 0
                for row in rows:
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
                        for td in soup_row.findAll('th'):
                            cell_dict = dict()
                            cell_dict["cell"] = str(td)
                            # cell_dict["text"] = [{"result": {"value": ''.join(td.stripped_strings)}}]
                            cell_dict["text"] = ''.join(td.stripped_strings)
                            avg_cell_len += len(cell_dict["text"])
                            cell_list.append(cell_dict)
                        for td in soup_row.findAll('td'):
                            cell_dict = dict()
                            cell_dict["cell"] = str(td)
                            # cell_dict["text"] = [{"result": {"value": ''.join(td.stripped_strings)}}]
                            cell_dict["text"] = ''.join(td.stripped_strings)
                            avg_cell_len += len(cell_dict["text"])
                            cell_list.append(cell_dict)
                        avg_row_len_dev += pstdev([len(x["text"]) for x in cell_list])
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
                features["avg_row_len"] = mean(row_len_list)
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
                        avg_col_len_dev += pstdev([len(x) for x in value])
                        no_of_cols_containing_num += 1 if contains_digits(whole_col) is True else 0
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
                context_before = ' '.join(gen_context(table.find_all_previous(string=True)))
                context_after = ' '.join(gen_context(table.find_all_next(string=True)))
                table_rep = gen_html(row_list)
                fingerprint = create_fingerprint(table_rep)
                data_table["context_before"] = context_before
                data_table["context_after"] = context_after
                data_table["fingerprint"] = fingerprint
                data_table['html'] = table_rep
                result_tables.append(data_table)
        return result_tables


def create_fingerprint(table):
    table = str(table)
    all_tokens = list(set(re.split('[^\w]+',table)))
    all_tokens = sorted(all_tokens)
    fingerprint = '-'.join(all_tokens)
    return fingerprint


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

def table_decompose(html_doc):
    soup = BeautifulSoup(html_doc, 'html.parser')
    tables = soup.findAll('table')
    for table in tables:
        rows = is_data_table(table, 2)
        if(rows != False):
            table.decompose()
    
    return soup