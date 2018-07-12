import sys
import os
import re
import argparse
import pyexcel
import numpy
from datetime import date, datetime
import pyexcel
import logging
import demjson
import json
import copy
import etk.timeseries.location_range as lr
import etk.timeseries.location_parser as lp
import etk.timeseries.time_series_region as tsr

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.WARN)

class SpreadsheetAnnotation(object):
    def __init__(self, annotation, fn):
        self.locparser = lp.LocationParser()

        self.properties = annotation['Properties']
        self.sheet_indices = self.locparser.parse_range(annotation['Properties']['sheet_indices'])

        self.metadata = self.parse_md(annotation['GlobalMetadata'])
        self.provenance = dict(filename=fn)

        self.timeseries_regions = []
        for rgn in annotation['TimeSeriesRegions']:
            self.timeseries_regions.append(self.parse_tsr(rgn))

    def parse_md(self, md_json):
        md_dict = {}
        for mdspec in md_json:
            mdname = mdspec['name']
            md_dict[mdname] = {}
            md_dict[mdname]['source'] = mdspec['source']
            if mdspec['source'] == 'cell':
                (md_dict[mdname]['row'], md_dict[mdname]['col']) = self.locparser.parse_coords(mdspec['loc'])
            if mdspec['source'] == 'const':
                md_dict[mdname]['val'] = mdspec['val']
        return md_dict

    def parse_tsr(self, tsr_json):
        orientation = tsr_json['orientation']
        series_range = None
        if orientation == 'row':
            series_range = self.locparser.parse_range(tsr_json['rows'])
        else:
            series_range = self.locparser.parse_range(tsr_json['cols'])

        data_range = self.locparser.parse_range(tsr_json['locs'])

        time_coords = {}
        time_coords['locs'] = self.locparser.parse_range(tsr_json['times']['locs'])
        time_coords['granularity'] = tsr_json['times']['granularity']
        if 'mode' in tsr_json['times']:
            time_coords['mode'] = tsr_json['times']['mode']
        else:
            time_coords['mode'] = None

        time_coords['post_process'] = tsr_json['times'].get('post_process')

        mdspec = self.parse_tsr_metadata(tsr_json['metadata'], orientation)

        return tsr.TimeSeriesRegion(orientation=orientation, series_range=series_range, data_range=data_range,
                                metadata_spec=mdspec, time_coordinates=time_coords, global_metadata=self.metadata,
                                provenance = self.provenance)

    def parse_tsr_metadata(self, md_json, orientation):
        md_dict = {}
        reverse_orientation = {'row': 'col', 'col': 'row'}
        for md_sec in md_json:
            name = md_sec['name']
            md_dict[name] = {}

            if 'source' in md_sec:
                md_dict[name]['source'] = md_sec['source']
            else:
                md_dict[name]['source'] = reverse_orientation[orientation]

            loc = None
            if 'loc' in md_sec:
                loc = md_sec['loc']

            if md_dict[name]['source'] == 'cell':
                md_dict[name]['loc'] = self.locparser.parse_coords(loc)

            elif md_dict[name]['source'] == 'row':
                md_dict[name]['loc'] = self.locparser.parse_range(loc)

            elif md_dict[name]['source'] == 'col':
                md_dict[name]['loc'] = self.locparser.parse_range(loc)

            elif md_dict[name]['source'] == 'const':
                md_dict[name]['val'] = md_sec['val']

            if 'mode' in md_sec:
                md_dict[name]['mode'] = md_sec['mode']
            else:
                md_dict[name]['mode'] = 'normal'

        return md_dict


class ExtractSpreadsheet(object):

    def __init__(self, spreadsheet_fn, annotations_fn):
        self.normalized_source_file = os.path.basename(spreadsheet_fn)
        self.book = pyexcel.get_book(file_name=spreadsheet_fn, auto_detect_datetime=False)
        self.annotations = self.load_annotations(annotations_fn)

    def process(self):
        timeseries = []
        for annotation in self.annotations:
            ssa = SpreadsheetAnnotation(annotation, self.normalized_source_file)
            parsed = []
            for anidx in ssa.sheet_indices:
                sheet = self.book.sheet_by_index(anidx)
                data = sheet.to_array()
                for rgn in ssa.timeseries_regions:
                    rgn.provenance['sheet']=anidx
                    for parsed_tsr in rgn.parse(data, sheet.name):
                        yield parsed_tsr
#                        parsed.append(parsed_tsr)
#                logging.debug("%s",parsed)
#            timeseries.append(parsed)
#        return timeseries
    def load_annotations(self,annotations_fn):
        anfile = open(annotations_fn)
        annotations_decoded = demjson.decode(anfile.read(), return_errors=True)
        for msg in annotations_decoded[1]:
            if msg.severity == "error":
                logging.error(msg.pretty_description())
        anfile.close()
        return annotations_decoded[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("annotation", help='Annotation of time series in custom JSON format')
    ap.add_argument("spreadsheet", help='Excel spreadsheet file')
    ap.add_argument("outfile", help='file to write results')
    args = ap.parse_args()
    es = ExtractSpreadsheet(args.spreadsheet, args.annotation)
    with open(args.outfile, 'w') as outfile:
            for timeseries in es.process():
                json.dump(timeseries, outfile)
                outfile.write("\n");

if __name__ == "__main__":
    main()
