# -*- coding: utf-8 -*-
from typing import List
from etk.extractor import Extractor, InputType
from etk.extraction import Extraction

from bs4 import BeautifulSoup
from bs4.element import Comment
import re
import copy
import pandas as pd
from zipfile import ZipFile
from requests import get

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


class EntityTableDataExtraction(Extractor):
    def __init__(self) -> None:
        Extractor.__init__(self,
                           input_type=InputType.OBJECT,
                           category="data",
                           name="DigEntityTableDataExtractor")
        self.glossaries = dict()

    def add_glossary(self, glossary: List[str], attr_name: str) -> None:
        """
        Adds a glossary for the given attribute name
        :param glossary: a list of possible mentions of the attribute name
        :param attr_name: the attribute name (field name)
        """
        self.glossaries[attr_name] = glossary

    def wrap_value_with_context(self, value: dict, field_name: str, start: int=0, end: int=0) -> Extraction:
        """Wraps the final result"""
        return Extraction(value, self.name, start_token=start, end_token=end, tag=field_name)

    def extract(self, table: dict) -> List[Extraction]:
        """
        :param table: a table extracted by table extractor, as a json object
        :return: list of all extractions from the input table
        """
        if table['features']['max_cols_in_a_row'] != 2 and table['features']['no_of_rows'] < 2:
            return []
        results = list()
        for row in table['rows']:
            if len(row['cells']) != 2:
                continue
            text = [row['cells'][0]['text'], row['cells'][1]['text']]
            for field_name in self.glossaries.keys():
                if self.cell_matches_dict(text[0], self.glossaries[field_name]):
                    results.append(self.wrap_value_with_context(text[1], field_name))
                if self.cell_matches_dict(text[1], self.glossaries[field_name]):
                    results.append(self.wrap_value_with_context(text[0], field_name))
        return results

    def cell_matches_dict(self, cell_text: str, glossary: List[str]) -> bool:
        if any([self.cell_matches_text(cell_text, x) for x in glossary]):
            return True
        return False

    def cell_matches_text(self, cell_text: str, text: str) -> bool:
        cell_text = cell_text.lower()
        text = text.lower()
        if text in cell_text and float(len(cell_text))/float(len(text)) < 1.5:
            return True
        return False


class TableExtraction:
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
        if table.thead:
            rows.extend(table.thead.findAll('tr', recursive=False))
        if table.tbody:
            rows.extend(table.tbody.findAll('tr', recursive=False))
        for tr in rows:
            if TableExtraction.is_data_row(tr):
                data_rows.append(tr)
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

    @staticmethod
    def convert_to_csv(jobj, outpath):
        with ZipFile(outpath, 'w') as myzip:
            for ti, t in enumerate(jobj['tables']):
                with myzip.open(f't{ti}.csv', 'w') as myfile:
                    tarr = [[c['text'] if c is not None else '' for c in r['cells']] for r in t['rows']]
                    csv = pd.DataFrame(tarr).to_csv(index=False, header=False)
                    myfile.write(csv.encode('utf-8'))
                myfile.close()

    @staticmethod
    def extract(input_data, expand_merged_cells=False, 
                input_format='html', output_format='json', 
                min_data_rows=1, output_path=None):
        """extract tables from input data.

        Args:
            input_data (str): input url/html.
            expand_merged_cells (bool): replicate the value of merged cells.
            input_format (str): the form of input data, {html, pdf}
            output_format (str): output format, {json, csv}
            min_data_rows (int): minimum number of rows for data tables.
            output_path (str): path to output file if output format is csv.

        Returns:
            bool: json object if output_format is json, None otherwise.
            
        Examples:

        >>> url = 'https://en.wikipedia.org/wiki/United_States'
        >>> html = requests.get(url).text
        >>> te = TableExtraction()
        >>> res = te.extract(html, expand_merged_cells=False, 
                        output_format='csv', output_path='test.zip')

        >>> res = te.extract(html)['tables'][0]['text']
        United States of America | \nFlag Coat of arms | \nMot ....
        
        >>> url = 'https://www.imf.org/~/media/Files/Publications/WP/wp1715.ashx'
        >>> te.extract(url, input_format='pdf',
                        expand_merged_cells=False, output_format='csv', 
                        output_path='test.csv')
        """
        if input_format == 'html':
            json_output = TableExtraction.extract_html(input_data, expand_merged_cells, min_data_rows)
            if output_format == 'json':
                return json_output
            elif output_format == 'csv':
                return TableExtraction.convert_to_csv(json_output, output_path)
        elif input_format == 'pdf':
            import tabula
            res = get(input_data)
            with open(output_path+".pdf", 'wb') as tempfile:
                tempfile.write(res.content)
            tempfile.close()
            tabula.convert_into(output_path+".pdf", output_path, output_format="zip", pages='all')

    @staticmethod
    def extract_html(html_doc, expand_merged_cells, min_data_rows=1):
        soup = BeautifulSoup(html_doc, 'html5lib')
        result_tables = list()
        tables = soup.findAll('table')
        for table_i, table in enumerate(tables):
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
            if rows != False:
                features = dict()
                row_len_list = list()
                avg_cell_len = 0
                avg_row_len_dev = 0
                
                num_rows = 0
                num_cols = 0
                max_cols = 0
                row_shift = 0
                
                ## detect merged cells and find table demension
                merged_cells = []
                row_spans = dict()
                for ri, row in enumerate(rows):
                    rshift = 0
                    num_rows += 1
                    num_cols = 0
                    col_shift = 0
                    ci = 0
                    for ci, c in enumerate(row.findAll(['td', 'th'])):
                        num_cols += 1
                        ci += col_shift+rshift
                        # shift the col index if there are any spanning cells above it
                        while ci in row_spans:
                            if row_spans[ci] <= 0:
                                del row_spans[ci]
                            else:
                                rshift += 1
                                row_spans[ci] -= 1
                                ci += 1

                        cspan = c.get('colspan')
                        rspan = c.get('rowspan')

                        # record spanned cell for later use
                        if cspan is not None and rspan is not None:
                            cspan = int(cspan)
                            rspan = int(rspan)
                            col_shift += cspan-1
                            for ii in range(ci, ci+cspan):
                                row_spans[ii] = rspan-1
                            merged_cells.append((ri, ri+rspan, ci, ci+cspan))
                        elif cspan is not None:
                            cspan = int(cspan)
                            col_shift += cspan-1
                            merged_cells.append((ri, ri+1, ci, ci+cspan))
                        elif rspan is not None:

                            rspan = int(rspan)
                            row_spans[ci] = rspan-1
                            merged_cells.append((ri, ri+rspan, ci, ci+1))
                    if max_cols < num_cols:
                        max_cols = num_cols
                    # update rowspan dict for columns not seen in this iteration
                    for k, v in row_spans.items():
                        if k > ci:
                            row_spans[k] -= 1
                if len(merged_cells) > 0:
                    max_cols = max(max_cols, max([x[3] for x in merged_cells]))
                    num_rows = max(len(rows), max([x[1] for x in merged_cells]))
                else:
                    num_rows = len(rows)
                ## create table array
                row_spans = dict()
                for ri, row in enumerate(rows):
                    row_data = ' '.join(row.stripped_strings)
                    row_data = row_data.replace("\\t", "").replace("\\r", "").replace("\\n", "")
                    
                    row_len_list.append(len(row_data))
                    row_tdcount = len(row.findAll('td')) + len(row.findAll('th'))
                    if row_tdcount > max_tdcount:
                        max_tdcount = row_tdcount
                    tdcount += row_tdcount
                    img_count += len(row.findAll('img'))
                    href_count += len(row.findAll('a'))
                    inp_count += len(row.findAll('input'))
                    sel_count += len(row.findAll('select'))
                    colspan_count += row_data.count("colspan")
                    colon_count += row_data.count(":")
                    len_row += 1
                    table_data += str(row)
                    
                    row_dict = dict()
                    newr = [None]*max_cols
                    shift = 0
                    rshift = 0
                    ci = 0
                    
                    for i, c in enumerate(row.findAll(['td', 'th'])):
                        ci = i+shift+rshift
                        while ci in row_spans:
                            if row_spans[ci] <= 0:
                                del row_spans[ci]
                            else:
                                rshift += 1
                                row_spans[ci] -= 1
                                ci += 1
                        for br in c.find_all("br"):
                            br.replace_with(" ")
                        for br in c.find_all("script"):
                            br.decompose()
                        cell_dict = dict()
                        cell_dict["cell"] = str(c)
                        cell_dict["text"] = ' '.join(c.stripped_strings)
                        cell_dict["id"] = 'row_{0}_col_{1}'.format(ri, ci)
                        
                        avg_cell_len += len(cell_dict["text"])
                        newr[ci] = cell_dict
                        cspan = c.get('colspan')
                        rspan = c.get('rowspan')

                        if cspan is not None and rspan is not None:
                            cspan = int(cspan)
                            rspan = int(rspan)
                            shift += cspan-1
                            for ii in range(ci, ci+cspan):
                                row_spans[ii] = rspan-1
                        elif rspan is not None:
                            rspan = int(rspan)
                            row_spans[ci] = rspan-1
                        elif cspan is not None:
                            cspan = int(cspan)
                            shift += cspan-1
                    for i in range(ci+1, max_cols):
                        if i in row_spans:
                            row_spans[i] -= 1
                    
                    avg_row_len_dev += TableExtraction.pstdev([len(x["text"]) if x else 0 for x in newr])
                    row_dict["cells"] = newr
                    row_dict["text"] = TableExtraction.row_to_text(newr)
                    row_dict["html"] = TableExtraction.row_to_html(newr)
                    row_dict["id"] = "row_{}".format(ri)
                    row_list.append(row_dict)
                if expand_merged_cells:
                    # replicate merged cells
                    N = len(row_list)
                    M = max_cols
                    for m in merged_cells:
                        if row_list[m[0]]['cells'][m[2]] is None:
                            print(m)
                        for ii in range(m[0], min(m[1], N)):
                            for jj in range(m[2], min(m[3], M)):
                                if ii == m[0] and jj == m[2]:
                                    continue
                                row_list[ii]['cells'][jj] = copy.deepcopy(row_list[m[0]]['cells'][m[2]])
                                row_list[ii]['cells'][jj]['id'] += '_span_row{}_col{}'.format(ii,jj)
                
                # To avoid division by zero
                if len_row == 0:
                    tdcount = 1

                features['merged_cells'] = merged_cells
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

                if colspan_count == 0.0 and \
                    len_row != 0 and \
                    (tdcount/(len_row * 1.0)) == max_tdcount:
                    col_data = dict()
                    for i in range(max_tdcount):
                        col_data['c_{0}'.format(i)] = []
                    soup_col = BeautifulSoup(table_data, 'html.parser')
                    for row in soup_col.findAll('tr'):
                        h_index = 0
                        h_bool = True
                        for col in row.findAll('th'):
                            col_content = ' '.join(col.stripped_strings)
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
                            col_content = ' '.join(col.stripped_strings)
                            if col_content is None:
                                d_index += 1
                                continue
                            else:
                                col_data['c_{0}'.format(d_index)].append(col_content)
                            d_index += 1

                    for key, value in col_data.items():
                        whole_col = ' '.join(value)
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
                data_table['html'] = str(table)
                data_table['text'] = TableExtraction.table_to_text(row_list)
                result_tables.append(data_table)
                table.decompose()
        return dict(tables=result_tables, html_text=TableExtraction.text_from_html(soup))

    @staticmethod
    def create_fingerprint(table):
        table = str(table)
        all_tokens = list(set(re.split('[^\w]+',table)))
        all_tokens = sorted(all_tokens)
        fingerprint = '-'.join(all_tokens)
        return fingerprint

    @staticmethod
    def row_to_html(cells):
        res = '<html><body><table>'
        for i, c in enumerate(cells):
            t = c['cell'] if c else ''
            res += t + '\n'
        res += '</table></body></html>'
        return res

    @staticmethod
    def row_to_text(cells):
        res = ''
        for i, c in enumerate(cells):
            t = c['text'] if c else ''
            res += t
            if i < len(cells)-1:
                res += ' | '
        return res

    @staticmethod
    def table_to_text(rows):
        res = ''
        for row in rows:
            for i, c in enumerate(row['cells']):
                t = c['text'] if c else ''
                res += t
                if i < len(row['cells']) - 1:
                    res += ' | '
            res += '\n'
        return res


    @staticmethod
    def gen_html(row_list):
        """ Return html table string from a list of data rows """
        table = "<table>"
        for row in row_list:
            table += "<tr>"
            cells = row["cells"]
            for c in cells:
                t = c['cell'] if c else ''
                table += t
            table += "</tr>"
        table += "</table>"
        return table

    @staticmethod
    def remove_tables(html_doc, min_data_rows = 1):
        soup = BeautifulSoup(html_doc, 'html.parser')
        tables = soup.findAll('table')
        for table in tables:
            rows = TableExtraction.is_data_table(table, min_data_rows)
            if rows != False:
                table.decompose()

        return soup

    @staticmethod
    def tag_visible(element):
        if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
            return False
        if isinstance(element, Comment):
            return False
        return True

    @staticmethod
    def text_from_html(soup):
        texts = soup.findAll(text=True)
        visible_texts = filter(TableExtraction.tag_visible, texts)
        # print([x.strip() for x in visible_texts])
        # exit(0)
        return u" ".join(t.strip() for t in visible_texts if t.strip() != "")


class TableExtractor(Extractor):
    """
    **Description**
        Adding explanation here

    Examples:
        ::

            table_extractor = TableExtractor()
            table_extractor.extract(html=html_str,
                                    return_text=True)

    """

    def __init__(self) -> None:
        Extractor.__init__(self,
                           input_type=InputType.TEXT,
                           category="content",
                           name="DigTableExtractor")
        self.tableExtractorInstance = TableExtraction()

    def _wrap_value_with_context(self, value: dict or str, field_name: str, start: int=0, end: int=0) -> Extraction:
        """Wraps the final result"""
        return Extraction(value, self.name, start_token=start, end_token=end, tag=field_name)

    def extract(self, html: str, return_text: bool = False, expand_merged_cells: bool = True) -> List[Extraction]:
        """
        Args:
            html (str): raw html of the page
            return_text (bool): if True, return the visible text in the page
                                removing all the data tables

        Returns:
            List[Extraction]: a list of Extractions

        """
        results = list()
        temp_res = self.tableExtractorInstance.extract(html, expand_merged_cells=expand_merged_cells)
        if return_text:
            results.append(self._wrap_value_with_context(temp_res['html_text'], "text_without_tables"))
        results.extend(map(lambda t: self._wrap_value_with_context(t, "tables"), temp_res['tables']))
        return results
