import sys
from datetime import date, datetime
import logging
import json
import copy
import decimal
import etk.timeseries.location_range as lr
import etk.timeseries.location_parser as lp
import hashlib
import re

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.WARN)


class DecimalJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalJSONEncoder, self).default(o)


class TimeSeriesRegion(object):
    def __init__(self, orientation='row', series_range=None, data_range=None, metadata_spec=None,
                 time_coordinates=None, global_metadata=None, granularity=None, provenance=None):
        self.orientation = orientation
        self.series_range = series_range
        self.data_range = data_range
        self.granularity = granularity
        self.metadata_spec = metadata_spec
        self.time_coordinates = time_coordinates
        self.global_metadata = global_metadata
        self.provenance = provenance
        self.time_series = []

    def parse(self, data, sheet_name):
        metadata = self.parse_global_metadata(data, sheet_name)
        self.parse_ts(data, metadata)
        return self.time_series

    def data_to_string(self, data):
        return str(data)

    def parse_global_metadata(self, data, sheet_name):
        metadata = {}
        for mdname, mdspec in self.global_metadata.items():
            if mdspec['source'] == 'sheet_name':
                metadata[mdname] = sheet_name
            elif mdspec['source'] == 'cell':
                metadata[mdname] = data[mdspec['row']][mdspec['col']]
            elif mdspec['source'] == 'const':
                metadata[mdname] = mdspec['val']
            else:
                logging.warn("Unknown metadata source %s", mdspec['source'])
        metadata['provenance'] = self.provenance
        return metadata

    def parse_tsr_metadata(self, metadata, data, tsidx):
        mds = self.metadata_spec
        md_modes = {}
        all_blank = True
        for md_name in mds:
            if mds[md_name]['mode'] == 'normal' or mds[md_name]['mode'] == 'backfill':
                if mds[md_name]['source'] == 'cell':
                    metadata[md_name] = data[mds[md_name]['loc'][0]][mds[md_name]['loc'][1]]
                    if not self.is_blank(metadata[md_name]):
                        all_blank = False
                elif mds[md_name]['source'] == 'const':
                    metadata[md_name] = mds[md_name]['val']
                else:
                    md_vals = []
                    for idx in mds[md_name]['loc']:
                        coords = self.orient_coords(tsidx, idx)
                        val = self.data_to_string(data[coords[0]][coords[1]])
                        md_vals.append(val)
                        if not self.is_blank(val):
                            all_blank = False
                    metadata[md_name] = " ".join(md_vals)
                if not metadata[md_name] and mds[md_name]['mode']=='backfill':
                    metadata[md_name] = self.time_series[-1]['metadata'][md_name]
            else:
                md_modes[mds[md_name]['mode']] = True
        if all_blank and ("inline" not in md_modes or not md_modes["inline"]):
            logging.debug("%s",metadata)
            raise IndexError("All metadata values blank for %d"%(tsidx))
        return md_modes

    def parse_inline_tsr_metadata(self, metadata, data, dataidx):
        mds = self.metadata_spec
        for md_name in mds:
            if mds[md_name]['mode'] == 'inline':
                md_vals = []
                for idx in mds[md_name]['loc']:
                    coords = self.orient_coords(idx, dataidx)
                    md_vals.append(self.data_to_string(data[coords[0]][coords[1]]))
                metadata[md_name] = " ".join(md_vals)

    def orient_coords(self, tsidx, dataidx):
        if self.orientation == 'row':
            return (tsidx, dataidx)
        else:
            return (dataidx, tsidx)

    def get_uid(self, metadata):
        md_str = json.dumps(metadata, sort_keys=True, cls=DecimalJSONEncoder).encode('utf-8')
        hash_object = hashlib.sha1(md_str)
        return hash_object.hexdigest()

    def generate_time_label(self, data, d_idx):
        time_labels = []
        for tc in self.time_coordinates['locs']:
            coords = self.orient_coords(tc, d_idx)
            val = self.data_to_string(data[coords[0]][coords[1]])
            if self.is_blank(val) and self.time_coordinates['mode'] == 'backfill':
                t_idx = d_idx - 1
                while t_idx > 0 and self.is_blank(val):
                    coords = self.orient_coords(tc, t_idx)
                    val = self.data_to_string(data[coords[0]][coords[1]])
                    t_idx -= 1
            time_labels.append(val)
        time_label = " ".join(time_labels)
        if self.time_coordinates['post_process']:
            func = eval('lambda v: ' + self.time_coordinates['post_process'])
            time_label = func(time_label)
        return self.process_time_span(time_label, self.time_coordinates['granularity'])

    def process_time_span(self, time_instant, granularity):
        # TODO: other granularities added (weekly :-?)
        granularities = {'yearly', 'monthly', 'quarterly'}
        if granularity not in granularities:
            return {'instant': time_instant}

        # TODO: other parsing date patterns to be added

        date_parts = time_instant.split('-')
        if granularity == 'yearly':
            time_span = {'start_time': self.fill_date_pattern(self.parse_yearly_date(time_instant))}
            time_span['end_time'] = self.parse_yearly_date(self.parse_yearly_date(date_parts[0]))#str(int(date_parts[0]))
            time_span['end_time'] += '-' + date_parts[1] if len(date_parts) > 1 else '-12'
            time_span['end_time'] += '-' + date_parts[2] if len(date_parts) > 2 else '-30'
            return {'span': time_span}
        if granularity == 'monthly':
            time_span = {'start_time': self.fill_date_pattern(self.parse_monthly_date(time_instant))}
        else:
            time_span = {'start_time': self.fill_date_pattern(time_instant)}
        month_offset = 3
        if granularity == 'monthly':  # the final choice of time span will be based on the given day
            month_offset = 1

        month_str = str(int(date_parts[1]) + month_offset)
        year_offset = 0
        if int(date_parts[1]) + month_offset >= 13:
            month_str = str((int(date_parts[1]) + month_offset) % 12 + 1)
            year_offset += 1
            time_span['end_time'] = str(int(date_parts[0]) + 1) + '-' + str(
                (int(date_parts[1]) + month_offset) % 12 + 1)

        if len(month_str) < 2:
            month_str = '0' + month_str
        time_span['end_time'] = date_parts[0] + '-' + month_str
        time_span['end_time'] += '-' + date_parts[2] if len(date_parts) > 2 else '-01'
        return {'span': time_span}


    def parse_yearly_date(self, time_label):
        try: #TODO: add unforseen date patterns here
            return re.search('(\d{4})', time_label)[0]
        except:
            logging.error('This date format is not recognized, yearly granularity reported {}'.format(time_label))
            return '2000' # only for preventing the extractor to crash


    def parse_monthly_date(self, time_label):
        month_kw = {"jan":1, "feb":2, "mar":3, "apr":4, "may":5, "jun":6, "jul":7, "aug":8, "sep":9, "oct":10, "nov":11, "dec":12,
            "january":1, "february":2, "march":3, "april":4, "june":6, "july":7, "august":8, "september":9, "october":10, "november":11, "december":12}
        try:
            year = re.search('(\d{4})', time_label).group(1)
            month = re.search('(^|[^\d])(\d{2})([^\d]|$)', time_label)
            if month != None:
                return year + '-' + month.group(2)
            for k in month_kw:
                if k in time_label.lower:
                    return year + '-' + str(month_kw[k])
            #TODO: add unforseen date patterns here
        except:
            logging.error('This date format is not recognized, monthly granularity reported {}'.format(time_label))
        logging.error('This date format is not recognized, monthly granularity reported {}'.format(time_label))
        return '1'



    def fill_date_pattern(self, time_instant):
        date_parts = time_instant.split('-')
        time_ins = str(date_parts[0])
        time_ins += '-' + date_parts[1] if len(date_parts) > 1 else '-01'
        time_ins += '-' + date_parts[2] if len(date_parts) > 2 else '-01'
        return time_ins

    def parse_ts(self, data, metadata):
        self.time_series = []
        for ts_idx in self.series_range:
            logging.debug("Parsing TS at %d", ts_idx)
            timeseries = []
            ts_metadata = copy.deepcopy(metadata)
            ts_metadata['provenance'][self.orientation] = ts_idx

            try:
                md_modes = self.parse_tsr_metadata(ts_metadata, data, ts_idx)
            except IndexError as ie:
                if type(self.series_range.curr_component()) is lr.LocationRangeInfiniteIntervalComponent:
                    logging.info("all blank metadata cells in infinite interval")
                    break
                else:
                    logging.error("metadata specifcation indexing error for time series index {}".format(ts_idx))
                    raise ie

            inline_md_curr = dict()
            inline_md_prev = None
            for d_idx in self.data_range:
                measurement = dict()
                time_label = ''
                try:
                    time_label = self.generate_time_label(data, d_idx)
                except IndexError as ie:
                    if type(self.data_range.curr_component()) is lr.LocationRangeInfiniteIntervalComponent:
                        break
                    else:
                        logging.error("metadata specifcation indexing error for data point index {}".format(d_idx))
                        raise ie

                if type(
                        self.data_range.curr_component()) is lr.LocationRangeInfiniteIntervalComponent and self.is_blank(
                        time_label):
                    logging.info("blank cell in infinite interval")
                    break

                # if inline metadata has changed (in auto-detect mode)
                # merge previous metadata
                # output old time series
                # re-initialize time series array
                if 'inline' in md_modes:
                    self.parse_inline_tsr_metadata(inline_md_curr, data, d_idx)
                    if inline_md_prev:
                        md_changed = False
                        for md_name in inline_md_prev:
                            if inline_md_curr[md_name] != inline_md_prev[md_name]:
                                md_changed = True

                        if md_changed:
                            new_metadata = dict(ts_metadata)
                            for md_name in inline_md_prev:
                                new_metadata[md_name] = inline_md_prev[md_name]

                            self.time_series.append({
                                'metadata': new_metadata,
                                'ts': timeseries
                            })
                            timeseries = []

                        inline_md_prev = inline_md_curr
                        inline_md_curr = dict()

                    else:
                        inline_md_prev = inline_md_curr
                        inline_md_curr = dict()

                coords = self.orient_coords(ts_idx, d_idx)
                measurement['value'] = data[coords[0]][coords[1]]
                measurement['time'] = time_label
                measurement['provenance'] = copy.deepcopy(ts_metadata['provenance'])
                measurement['provenance']['row'] = coords[0]
                measurement['provenance']['col'] = coords[1]
                measurement['uid'] = self.get_uid(measurement)
                timeseries.append(measurement)
            ts_metadata['uid'] = self.get_uid(ts_metadata)
            self.time_series.append(dict(metadata=ts_metadata, ts=timeseries))

    def is_blank(self, data):
        return len(str(data).strip()) == 0

