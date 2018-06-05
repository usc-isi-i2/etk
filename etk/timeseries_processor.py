import etk.timeseries.extract_spreadsheet as extractSpreadsheet
from typing import List
from etk.document import Document
from etk.etk_exceptions import InvalidArgumentsError


class TimeseriesProcessor(object):
    def __init__(self, etk, **mapping_spec) -> None:
        self.etk = etk
        # < annotation > < spreadsheet > < output >
        if mapping_spec['annotation'] is None:
            #  TODO: adding auto infer annotation code
            pass
        else:
            self.annotation = mapping_spec['annotation']

        if mapping_spec['spreadsheet'] is None:
            raise InvalidArgumentsError("for argument 'spreadsheet', please specify spreadsheet path")
            # print("Please specify spreadsheet path")
        else:
            self.spreadsheet = mapping_spec['spreadsheet']

    def timeseries_extractor(self, file_name: str = None,
                             data_set: str = None) -> List[Document]:
        es = extractSpreadsheet.ExtractSpreadsheet(self.spreadsheet, self.annotation)
        timeseries = es.process()

        return self.create_documents(json_objs=timeseries, file_name=file_name, data_set=data_set)

    def create_documents(self, json_objs: object,
                         file_name: str = None,
                         data_set: str = None, ) -> List[Document]:
        documents = list()

        for json_obj in json_objs:
            cdr_doc = json_obj

            if file_name is not None:
                cdr_doc['file_name'] = file_name
            if data_set is not None:
                cdr_doc['data_set'] = data_set

            documents.append(self.etk.create_document(cdr_doc))

        return documents
