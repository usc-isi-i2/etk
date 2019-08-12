from typing import List, Dict, Tuple

import copy
import re
import pyexcel

from etk.etk import ETK
from etk.extractor import Extractor, InputType
from etk.extraction import Extraction


class ExcelExtractor(Extractor):
    """
    **Description**
        This class is a wrapper of pyexcel. By defining 'variables' argument,
        this extractor is able to extract multiple information and store them
        into different field at once.

    Examples:
        ::

            excel_extractor = ExcelExtractor()
            excel_extractor.extract(file_name:path_to_excel,
                                    sheet_name:sheet_name,
                                    region:['A,1', 'Z,10'],
                                    variables={
                                        # get current col & row for field1
                                        'field1': '$col,$row',
                                        # get fixed cell of $A,$5 for field2
                                        'field2': '$A,$5'
                                        })

    """

    _re_row_identifier = re.compile(r'(\$[0-9]+)')
    _re_col_identifier = re.compile(r'(\$[A-Za-z]+)')

    def __init__(self, etk: ETK = None, extractor_name: str = 'excel extractor') -> None:
        Extractor.__init__(self,
                           input_type=InputType.TEXT,
                           category="data extractor",
                           name=extractor_name)
        self._etk = etk

    def extract(self, file_name: str, sheet_name: str, region: List, variables: Dict) -> List[Extraction]:
        """
        Args:
            file_name (str): file name
            sheet_name (str): sheet name
            region (List[]): from upper left cell to bottom right cell, e.g., ['A,1', 'Z,10']
            variables (Dict): key is variable name, value can be:
                              1. a single expression 2. comma separated expression, will be treated as location
                              $row, $col are built-in variables can be used in expression
                              constant row and column value can be noted as $NAME (e.g., $1, $10, $A, $GG)
        
        Returns:
            List[Extraction] : A list of extracted variables dictionary
            
        """
        extractions = []

        book = pyexcel.get_book(file_name=file_name)
        sheet = book[sheet_name]
        region = [ExcelExtractor._excel_coord_to_location(coord) for coord in region]
        r = region[0][0]
        # per row
        for row in sheet.region(region[0], region[1]):
            c = region[0][1]
            # per col
            for col in row:
                var = copy.deepcopy(variables)
                # per variable
                for k, v in var.items():
                    parsed_v = ExcelExtractor._parse_variable(v, r, c)
                    if len(parsed_v) == 1:  # normal variable
                        var[k] = parsed_v[0]
                    else:  # location
                        rr, cc = parsed_v
                        var[k] = sheet[rr, cc]
                extractions.append(var)

                c += 1
            r += 1

        return extractions

    @staticmethod
    def _col_name_to_num(name: str) -> int:
        name = name.upper()
        pow = 1
        col_num = 0
        for letter in name[::-1]:
            col_num += (int(letter, 36) - 9) * pow
            pow *= 26
        return col_num - 1

    @staticmethod
    def _row_name_to_num(name: str) -> int:
        try:
            num = int(name) - 1
            if num >= 0:
                return num
            raise ValueError('Invalid row name')
        except:
            raise ValueError('Invalid row name')

    @staticmethod
    def _excel_coord_to_location(s: str) -> Tuple:
        ss = s.split(',')
        return ExcelExtractor._row_name_to_num(ss[1]), ExcelExtractor._col_name_to_num(ss[0])

    @staticmethod
    def _parse_variable(s: str, curr_row: int, curr_col: int) -> Tuple:
        '''
        $A,$2 <- constant col and row
        $row,$2 <- current col, row 2
        $A+1,$2 <- col A + 1 = 2, row 2
        $row+1,$2 <- current col + 1, row 2
        $A,$2-1 <-- col A, row 2 - 1 = 1
        '''

        def parse_expression(ss, curr_row, curr_col):
            ss = ss.replace('$row', str(curr_row))
            ss = ss.replace('$col', str(curr_col))
            ss = ExcelExtractor._re_row_identifier.sub(
                lambda x: str(ExcelExtractor._row_name_to_num(x.group()[1:])) if len(x.group()) > 0 else '', ss)
            ss = ExcelExtractor._re_col_identifier.sub(
                lambda x: str(ExcelExtractor._col_name_to_num(x.group()[1:])) if len(x.group()) > 0 else '', ss)
            return eval(ss)

        ss = s.split(',')
        if len(ss) == 1:
            return parse_expression(ss[0], curr_row, curr_col),
        elif len(ss) == 2:
            rr, cc = (ss[1], ss[0])
            return parse_expression(rr, curr_row, curr_col), parse_expression(cc, curr_row, curr_col)
        else:
            raise ValueError('Invalid variable')
