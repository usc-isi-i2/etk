import unittest, json
from etk.csv_processor import CsvProcessor
from etk.etk import ETK
from etk.knowledge_graph import KGSchema
import pandas as pd
import numpy as np

csv_str = """text,with,Polish,non-Latin,lettes
1,2,3,4,5,6
a,b,c,d,e,f

gęś,zółty,wąż,idzie,wąską,dróżką,
,b,c,s,w,f
"""

kg_schema = KGSchema(json.load(open('etk/unit_tests/ground_truth/test_config.json')))

etk = ETK(kg_schema=kg_schema)


class TestCsvProcessor(unittest.TestCase):
    def test_csv_str_with_all_args(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     heading_row=1,
                                     content_start_row=2,
                                     heading_columns=(1, 3),
                                     content_end_row=3,
                                     ends_with_blank_row=True,
                                     remove_leading_empty_rows=True,
                                     required_columns=['text'])

        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(table_str=csv_str, dataset='test_csv_str_with_all_args')]

        expected_docs = [{'text': '1', 'with': '2', 'Polish': '3', 'non-Latin': '4', 'lettes': '5',
                          'dataset': 'test_csv_str_with_all_args'},
                         {'text': 'a', 'with': 'b', 'Polish': 'c', 'non-Latin': 'd', 'lettes': 'e',
                          'dataset': 'test_csv_str_with_all_args'}]

        self.assertEqual(test_docs, expected_docs)

    def test_csv_str_with_ends_with_blank_row_false(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     heading_row=1,
                                     content_start_row=2,
                                     heading_columns=(1, 3),
                                     content_end_row=4,
                                     ends_with_blank_row=False,
                                     remove_leading_empty_rows=True,
                                     required_columns=['text'])

        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(table_str=csv_str, dataset='test_set')]

        expected_docs = [
            {'text': '1', 'with': '2', 'Polish': '3', 'non-Latin': '4', 'lettes': '5', 'dataset': 'test_set'},
            {'text': 'a', 'with': 'b', 'Polish': 'c', 'non-Latin': 'd', 'lettes': 'e', 'dataset': 'test_set'}]

        self.assertEqual(test_docs, expected_docs)

    def test_csv_file_with_no_header(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     content_start_row=1,
                                     content_end_row=4,
                                     ends_with_blank_row=False,
                                     remove_leading_empty_rows=True)
        filename = 'etk/unit_tests/ground_truth/sample_csv.csv'

        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(filename=filename, dataset='test_set')]

        expected_docs = [{'C0': 'name1', 'C1': 'name2', 'file_name': 'etk/unit_tests/ground_truth/sample_csv.csv',
                          'dataset': 'test_set'}]

        self.assertEqual(test_docs, expected_docs)

    def test_csv_file_with_no_header_not_ends_with_blank_row(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     content_start_row=1,
                                     content_end_row=8,
                                     ends_with_blank_row=False,
                                     remove_leading_empty_rows=True)
        filename = 'etk/unit_tests/ground_truth/sample_csv.csv'

        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(filename=filename, dataset='test_set')]

        expected_docs = [{'C0': '', 'C1': 'name1', 'C2': 'name2', 'C3': '', 'C4': '',
                          'file_name': 'etk/unit_tests/ground_truth/sample_csv.csv', 'dataset': 'test_set'},
                         {'C0': 'col11', 'C1': 'col12', 'C2': 'col13', 'C3': '', 'C4': 'col15',
                          'file_name': 'etk/unit_tests/ground_truth/sample_csv.csv', 'dataset': 'test_set'},
                         {'C0': 'col21', 'C1': 'col22', 'C2': 'col23', 'C3': 'col24', 'C4': 'col25',
                          'file_name': 'etk/unit_tests/ground_truth/sample_csv.csv', 'dataset': 'test_set'},
                         {'C0': 'col31', 'C1': 'col32', 'C2': 'col33', 'C3': 'col34', 'C4': 'col35',
                          'file_name': 'etk/unit_tests/ground_truth/sample_csv.csv', 'dataset': 'test_set'}]

        self.assertEqual(test_docs, expected_docs)

    def test_real_csv_file_1(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     heading_row=1,
                                     content_end_row=4,
                                     ends_with_blank_row=False)

        file_path = 'etk/unit_tests/ground_truth/acled_raw_data.csv'

        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(filename=file_path, dataset='test_set')]

        expected_docs = [{'data_id': 336907, 'iso': 180, 'event_id_cnty': 'DRC11776', 'event_id_no_cnty': 11776,
                          'event_date': '2018-01-13', 'year': 2018, 'time_precision': 1, 'event_type':
                              'Battle-No change of territory',
                          'actor1': 'Military Forces of Democratic Republic of Congo (2001-)',
                          'assoc_actor_1': '', 'inter1': 1, 'actor2': 'ADF: Allied Democratic Forces',
                          'assoc_actor_2': '',
                          'inter2': 2, 'interaction': 12, 'region': 'Central Africa',
                          'country': 'Democratic Republic of Congo',
                          'admin1': 'Nord-Kivu', 'admin2': 'Nord-Kivu', 'admin3': 'Oicha', 'location': 'Oicha',
                          'latitude': '0.7',
                          'longitude': '29.5167', 'geo_precision': 1, 'source': 'Radio Okapi',
                          'source_scale': 'Subnational',
                          'notes': "Presumed FARDC attacked the ADF in the periphery of Oicha on January 13th. Shots were heard "
                                   "in the locality and it is suspected that the FARDC are attacking the 'death triangle' situated in between "
                                   "Mbau, Kamango and Eringeti. The reports are not confirmed by the military.",
                          'fatalities': 0,
                          'timestamp': 1516117305, 'file_name': 'etk/unit_tests/ground_truth/acled_raw_data.csv',
                          'dataset': 'test_set'},
                         {'data_id': 336908, 'iso': 180, 'event_id_cnty': 'DRC11777', 'event_id_no_cnty': 11777,
                          'event_date': '2018-01-13', 'year': 2018, 'time_precision': 1,
                          'event_type': 'Battle-No change of territory',
                          'actor1': 'Military Forces of Democratic Republic of Congo (2001-)', 'assoc_actor_1': '',
                          'inter1': 1,
                          'actor2': 'ADF: Allied Democratic Forces', 'assoc_actor_2': '', 'inter2': 2,
                          'interaction': 12,
                          'region': 'Central Africa', 'country': 'Democratic Republic of Congo', 'admin1': 'Nord-Kivu',
                          'admin2': 'Beni', 'admin3': 'Beni', 'location': 'Beni', 'latitude': '0.49658',
                          'longitude': '29.4654',
                          'geo_precision': 1, 'source': 'Reuters; Radio Okapi', 'source_scale': 'Subnational',
                          'notes': 'The FARDC launched, on January 13th, an offensive against the ADF in Beni and Lubero, '
                                   'in response to the recents attacks by the group. Gunfires and explosions were heard '
                                   'in Beni all throughout Saturday (13th).', 'fatalities': 0, 'timestamp': 1516117305,
                          'file_name': 'etk/unit_tests/ground_truth/acled_raw_data.csv', 'dataset': 'test_set'},
                         {'data_id': 336909, 'iso': 180, 'event_id_cnty': 'DRC11778', 'event_id_no_cnty': 11778,
                          'event_date': '2018-01-13', 'year': 2018, 'time_precision': 1,
                          'event_type': 'Battle-No change '
                                        'of territory',
                          'actor1': 'Military Forces of Democratic Republic of Congo (2001-)',
                          'assoc_actor_1': '', 'inter1': 1, 'actor2': 'ADF: Allied Democratic Forces',
                          'assoc_actor_2': '',
                          'inter2': 2, 'interaction': 12, 'region': 'Central Africa',
                          'country': 'Democratic Republic of Congo',
                          'admin1': 'Nord-Kivu', 'admin2': 'Nord-Kivu', 'admin3': 'Lubero', 'location': 'Lubero',
                          'latitude': '-0.15867', 'longitude': '29.2386', 'geo_precision': 1,
                          'source': 'Reuters; Radio Okapi',
                          'source_scale': 'Subnational', 'notes': 'The FARDC launched, on January 13th, an offensive '
                                                                  'against the ADF in Beni and Lubero, in response to the recents attacks by the group.',
                          'fatalities': 0, 'timestamp': 1516117305,
                          'file_name': 'etk/unit_tests/ground_truth/acled_raw_data.csv',
                          'dataset': 'test_set'}]

        self.assertEqual(test_docs, expected_docs)

    def test_real_csv_file_2(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     heading_row=2,
                                     content_start_row=10,
                                     content_end_row=13)

        file_path = 'etk/unit_tests/ground_truth/masie_4km_allyears_extent_sqkm.csv'

        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(filename=file_path, dataset='test_set')]

        expected_docs = [
            {'yyyyddd': 2006008, ' (0) Northern_Hemisphere': '13536736.84', ' (1) Beaufort_Sea': ' 1069710.81',
             ' (2) Chukchi_Sea': '  966006.16', ' (3) East_Siberian_Sea': ' 1087102.72',
             ' (4) Laptev_Sea': '  897773.37',
             ' (5) Kara_Sea': '  927602.17', ' (6) Barents_Sea': '  474574.82', ' (7) Greenland_Sea': '  590029.18',
             ' (8) Baffin_Bay_Gulf_of_St._Lawrence': ' 1005790.38', ' (9) Canadian_Archipelago': '  852715.31',
             ' (10) Hudson_Bay': ' 1260779.00', ' (11) Central_Arctic': ' 3240326.47',
             ' (12) Bering_Sea': '  692832.54',
             ' (13) Baltic_Sea': '   21327.46', ' (14) Sea_of_Okhotsk': '  424563.54',
             ' (15) Yellow_Sea': '   14830.45',
             ' (16) Cook_Inlet': '    8202.95',
             'file_name': 'etk/unit_tests/ground_truth/masie_4km_allyears_extent_sqkm.csv',
             'dataset': 'test_set'},
            {'yyyyddd': 2006009, ' (0) Northern_Hemisphere': '13536887.64', ' (1) Beaufort_Sea': ' 1069710.81',
             ' (2) Chukchi_Sea': '  966006.16', ' (3) East_Siberian_Sea': ' 1087102.72',
             ' (4) Laptev_Sea': '  897773.37',
             ' (5) Kara_Sea': '  927602.17', ' (6) Barents_Sea': '  474574.82', ' (7) Greenland_Sea': '  590029.18',
             ' (8) Baffin_Bay_Gulf_of_St._Lawrence': ' 1005790.38', ' (9) Canadian_Archipelago': '  852715.31',
             ' (10) Hudson_Bay': ' 1260779.00', ' (11) Central_Arctic': ' 3240326.47',
             ' (12) Bering_Sea': '  692832.54',
             ' (13) Baltic_Sea': '   21478.25', ' (14) Sea_of_Okhotsk': '  424563.54',
             ' (15) Yellow_Sea': '   14830.45',
             ' (16) Cook_Inlet': '    8202.95',
             'file_name': 'etk/unit_tests/ground_truth/masie_4km_allyears_extent_sqkm.csv',
             'dataset': 'test_set'},
            {'yyyyddd': 2006010, ' (0) Northern_Hemisphere': '13505426.35', ' (1) Beaufort_Sea': ' 1069710.81',
             ' (2) Chukchi_Sea': '  966006.16', ' (3) East_Siberian_Sea': ' 1087102.72',
             ' (4) Laptev_Sea': '  897773.37',
             ' (5) Kara_Sea': '  933999.29', ' (6) Barents_Sea': '  448185.27', ' (7) Greenland_Sea': '  588279.64',
             ' (8) Baffin_Bay_Gulf_of_St._Lawrence': ' 1016857.87', ' (9) Canadian_Archipelago': '  852715.31',
             ' (10) Hudson_Bay': ' 1260779.00', ' (11) Central_Arctic': ' 3217380.82',
             ' (12) Bering_Sea': '  705348.17',
             ' (13) Baltic_Sea': '   21493.81', ' (14) Sea_of_Okhotsk': '  414191.19',
             ' (15) Yellow_Sea': '   14830.45',
             ' (16) Cook_Inlet': '    8202.95',
             'file_name': 'etk/unit_tests/ground_truth/masie_4km_allyears_extent_sqkm.csv',
             'dataset': 'test_set'},
            {'yyyyddd': 2006011, ' (0) Northern_Hemisphere': '13493030.93', ' (1) Beaufort_Sea': ' 1069710.81',
             ' (2) Chukchi_Sea': '  966006.16', ' (3) East_Siberian_Sea': ' 1087102.72',
             ' (4) Laptev_Sea': '  897773.37',
             ' (5) Kara_Sea': '  933999.29', ' (6) Barents_Sea': '  448185.27', ' (7) Greenland_Sea': '  588279.64',
             ' (8) Baffin_Bay_Gulf_of_St._Lawrence': ' 1013091.28', ' (9) Canadian_Archipelago': '  852715.31',
             ' (10) Hudson_Bay': ' 1260779.00', ' (11) Central_Arctic': ' 3217380.82',
             ' (12) Bering_Sea': '  696719.33',
             ' (13) Baltic_Sea': '   21493.81', ' (14) Sea_of_Okhotsk': '  414191.19',
             ' (15) Yellow_Sea': '   14830.45',
             ' (16) Cook_Inlet': '    8202.95',
             'file_name': 'etk/unit_tests/ground_truth/masie_4km_allyears_extent_sqkm.csv',
             'dataset': 'test_set'}]

        self.assertEqual(test_docs, expected_docs)

    def test_tab_file(self):
        csv_processor = CsvProcessor(etk=etk,
                                     heading_row=1)
        test_str = 'Event ID\tEvent Date\tSource Name\tSource Sectors\tSource Country\tEvent Text\tCAMEO Code\tIntensity\tTarget Name\tTarget Sectors\tTarget Country\tStory ID\tSentence Number\tPublisher\tCity\tDistrict\tProvince\tCountry\tLatitude\tLongitude\n926685\t1995-01-01\tExtremist (Russia)\tRadicals / Extremists / Fundamentalists,Dissident\tRussian Federation\tPraise or endorse\t051\t3.4\tBoris Yeltsin\tElite,Executive,Executive Office,Government\tRussian Federation\t28235806\t5\tThe Toronto Star\tMoscow\t\tMoskva\tRussian Federation\t55.7522\t37.6156\n926687\t1995-01-01\tGovernment (Bosnia and Herzegovina)\tGovernment\tBosnia and Herzegovina\tExpress intent to cooperate\t030\t4\tCitizen (Serbia)\tGeneral Population / Civilian / Social,Social\tSerbia\t28235807\t1\tThe Toronto Star\t\t\tBosnia\tBosnia and Herzegovina\t44\t18\n926686\t1995-01-01\tCitizen (Serbia)\tGeneral Population / Civilian / Social,Social\tSerbia\tExpress intent to cooperate\t030\t4\tGovernment (Bosnia and Herzegovina)\tGovernment\tBosnia and Herzegovina\t28235807\t1\tThe Toronto Star\t\t\tBosnia\tBosnia and Herzegovina\t44\t18\n926688\t1995-01-01\tCanada\t\tCanada\tPraise or endorse\t051\t3.4\tCity Mayor (Canada)\tGovernment,Local,Municipal\tCanada\t28235809\t3\tThe Toronto Star\t\t\tOntario\tCanada\t49.2501\t-84.4998\n'
        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(filename='testfile.tab', file_type='tsv', file_content=test_str,
                                                     dataset='test_set')]
        expected_docs = [{'Event ID': 926685, 'Event Date': '1995-01-01', 'Source Name': 'Extremist (Russia)',
                          'Source Sectors': 'Radicals / Extremists / Fundamentalists,Dissident',
                          'Source Country': 'Russian Federation', 'Event Text': 'Praise or endorse',
                          'CAMEO Code': '051', 'Intensity': '3.4', 'Target Name': 'Boris Yeltsin',
                          'Target Sectors': 'Elite,Executive,Executive Office,Government',
                          'Target Country': 'Russian Federation', 'Story ID': 28235806, 'Sentence Number': 5,
                          'Publisher': 'The Toronto Star', 'City': 'Moscow', 'District': '', 'Province': 'Moskva',
                          'Country': 'Russian Federation', 'Latitude': '55.7522', 'Longitude': '37.6156',
                          'file_name': 'testfile.tab', 'dataset': 'test_set'},
                         {'Event ID': 926687, 'Event Date': '1995-01-01',
                          'Source Name': 'Government (Bosnia and Herzegovina)', 'Source Sectors': 'Government',
                          'Source Country': 'Bosnia and Herzegovina', 'Event Text': 'Express intent to cooperate',
                          'CAMEO Code': '030', 'Intensity': 4, 'Target Name': 'Citizen (Serbia)',
                          'Target Sectors': 'General Population / Civilian / Social,Social', 'Target Country': 'Serbia',
                          'Story ID': 28235807, 'Sentence Number': 1, 'Publisher': 'The Toronto Star', 'City': '',
                          'District': '', 'Province': 'Bosnia', 'Country': 'Bosnia and Herzegovina', 'Latitude': 44,
                          'Longitude': 18, 'file_name': 'testfile.tab', 'dataset': 'test_set'},
                         {'Event ID': 926686, 'Event Date': '1995-01-01', 'Source Name': 'Citizen (Serbia)',
                          'Source Sectors': 'General Population / Civilian / Social,Social', 'Source Country': 'Serbia',
                          'Event Text': 'Express intent to cooperate', 'CAMEO Code': '030', 'Intensity': 4,
                          'Target Name': 'Government (Bosnia and Herzegovina)', 'Target Sectors': 'Government',
                          'Target Country': 'Bosnia and Herzegovina', 'Story ID': 28235807, 'Sentence Number': 1,
                          'Publisher': 'The Toronto Star', 'City': '', 'District': '', 'Province': 'Bosnia',
                          'Country': 'Bosnia and Herzegovina', 'Latitude': 44, 'Longitude': 18,
                          'file_name': 'testfile.tab', 'dataset': 'test_set'},
                         {'Event ID': 926688, 'Event Date': '1995-01-01', 'Source Name': 'Canada', 'Source Sectors': '',
                          'Source Country': 'Canada', 'Event Text': 'Praise or endorse', 'CAMEO Code': '051',
                          'Intensity': '3.4', 'Target Name': 'City Mayor (Canada)',
                          'Target Sectors': 'Government,Local,Municipal', 'Target Country': 'Canada',
                          'Story ID': 28235809, 'Sentence Number': 3, 'Publisher': 'The Toronto Star', 'City': '',
                          'District': '', 'Province': 'Ontario', 'Country': 'Canada', 'Latitude': '49.2501',
                          'Longitude': '-84.4998', 'file_name': 'testfile.tab', 'dataset': 'test_set'}]
        self.assertEqual(test_docs, expected_docs)

    def test_real_excel_with_sheetname(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     heading_row=1,
                                     content_start_row=10,
                                     content_end_row=12)

        file_path = 'etk/unit_tests/ground_truth/NST-Main Sheet.xlsx'

        sheets_name = 'NST Main Dataset'

        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(filename=file_path, sheet_name=sheets_name, dataset='test_set')]

        expected_docs = [{'Title': 'David Usman and  Shot Dead', 'Date': '2011-06-07',
                          'Community (city,town, ward)': 'Maiduguri', 'LGA': 'Maiduguri', 'State': 'Borno',
                          'Total Deaths': 2,
                          'Boko Haram (P)': 'Boko Haram', 'State Actor (P)': '',
                          'Sectarian Actor (excluding BH) (P)': '',
                          'Other Armed Actor (P)': '', 'Kidnapper (P)': '', 'Robber (P)': '', 'Other (P)': '',
                          'Election-related Actor (P)': '', 'Cameroon State Actor (P)': '', 'Boko Haram (V)': '',
                          'State Actor (V)': '',
                          'Sectarian Actor (V)': 2, 'Other Armed Actor (V)': '', 'Political Actor (V)': '',
                          'Kidnapper (V)': '',
                          'Kidnapee (V)': '', 'Robber (V)': '', 'Journalist (V)': '', 'Civilian (V)': '',
                          'Election-related Actor (V)': '',
                          'Cameroon State Actor': '', 'Bomb': '', 'Gun': 'Gun', 'Machete': '', 'Suicide Bombing': '',
                          'Other Weapon': '', 'TK': 'Targeted Killing', 'Drinking Establishment': '',
                          'Goverment Building': '',
                          'Church': '', 'Mosque': '', 'Bank': '', 'School': '', 'Other Location': 'Other', 'Notes': '',
                          'Sources 1': 'http://allafrica.com/stories/201106100373.html',
                          'Sources 2': 'http://www.bbc.co.uk/news/world-africa-13724349',
                          'Sources 3': '', 'Latitude': '', 'Longitude': '',
                          'full place name': 'Maiduguri, Borno, Nigeria',
                          'country': 'Nigeria', 'file_name': 'etk/unit_tests/ground_truth/NST-Main Sheet.xlsx',
                          'dataset': 'test_set'},
                         {'Title': 'Explosion, Firefight at Gwange Police Station', 'Date': '2011-06-07',
                          'Community (city,town, ward)': 'Gwange Police Station, Maiduguri', 'LGA': 'Maiduguri',
                          'State': 'Borno',
                          'Total Deaths': 3, 'Boko Haram (P)': 'Boko Haram', 'State Actor (P)': 'State Actor',
                          'Sectarian Actor (excluding BH) (P)': '', 'Other Armed Actor (P)': '', 'Kidnapper (P)': '',
                          'Robber (P)': '', 'Other (P)': '', 'Election-related Actor (P)': '',
                          'Cameroon State Actor (P)': '',
                          'Boko Haram (V)': 3, 'State Actor (V)': '', 'Sectarian Actor (V)': '',
                          'Other Armed Actor (V)': '',
                          'Political Actor (V)': '', 'Kidnapper (V)': '', 'Kidnapee (V)': '', 'Robber (V)': '',
                          'Journalist (V)': '',
                          'Civilian (V)': '', 'Election-related Actor (V)': '', 'Cameroon State Actor': '',
                          'Bomb': 'Bomb',
                          'Gun': 'Gun', 'Machete': '', 'Suicide Bombing': '', 'Other Weapon': '', 'TK': '',
                          'Drinking Establishment': '',
                          'Goverment Building': 'Government Building', 'Church': '', 'Mosque': '', 'Bank': '',
                          'School': '',
                          'Other Location': '', 'Notes': '',
                          'Sources 1': 'http://www.google.com/hostednews/afp/article/ALeqM5hofvKayKKAFFtiX9-Ic5bG2ptVmg?docId=CNG.fafcacea0287fbeab90256732f165e1e.771',
                          'Sources 2': 'http://news.xinhuanet.com/english2010/world/2011-06/08/c_13915959.htm',
                          'Sources 3': '',
                          'Latitude': '', 'Longitude': '', 'full place name': 'Maiduguri, Borno, Nigeria',
                          'country': 'Nigeria',
                          'file_name': 'etk/unit_tests/ground_truth/NST-Main Sheet.xlsx', 'dataset': 'test_set'},
                         {'Title': 'Explosions at Dandal Police Station', 'Date': '2011-06-07',
                          'Community (city,town, ward)': 'Dandal Police Station, Maiduguri', 'LGA': 'Maiduguri',
                          'State': 'Borno', 'Total Deaths': 0, 'Boko Haram (P)': 'Boko Haram', 'State Actor (P)': '',
                          'Sectarian Actor (excluding BH) (P)': '', 'Other Armed Actor (P)': '', 'Kidnapper (P)': '',
                          'Robber (P)': '', 'Other (P)': '', 'Election-related Actor (P)': '',
                          'Cameroon State Actor (P)': '',
                          'Boko Haram (V)': '', 'State Actor (V)': 0, 'Sectarian Actor (V)': '',
                          'Other Armed Actor (V)': '',
                          'Political Actor (V)': '', 'Kidnapper (V)': '', 'Kidnapee (V)': '', 'Robber (V)': '',
                          'Journalist (V)': '', 'Civilian (V)': '', 'Election-related Actor (V)': '',
                          'Cameroon State Actor': '',
                          'Bomb': 'Bomb', 'Gun': '', 'Machete': '', 'Suicide Bombing': '', 'Other Weapon': '', 'TK': '',
                          'Drinking Establishment': '', 'Goverment Building': 'Government Building', 'Church': '',
                          'Mosque': '',
                          'Bank': '', 'School': '', 'Other Location': '', 'Notes': '',
                          'Sources 1': 'http://news.xinhuanet.com/english2010/world/2011-06/08/c_13915959.htm',
                          'Sources 2': 'http://www.google.com/hostednews/afp/article/ALeqM5hofvKayKKAFFtiX9-Ic5bG2ptVmg?docId=CNG.fafcacea0287fbeab90256732f165e1e.771',
                          'Sources 3': '', 'Latitude': '', 'Longitude': '',
                          'full place name': 'Maiduguri, Borno, Nigeria',
                          'country': 'Nigeria', 'file_name': 'etk/unit_tests/ground_truth/NST-Main Sheet.xlsx',
                          'dataset': 'test_set'}]

        self.assertEqual(test_docs, expected_docs)

    def test_real_excel_without_sheetname(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     heading_row=1,
                                     content_start_row=10,
                                     content_end_row=12)

        file_path = 'etk/unit_tests/ground_truth/NST-Main Sheet.xlsx'

        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(filename=file_path, dataset='test_set')]

        expected_docs = []

        self.assertEqual(test_docs, expected_docs)

    def test_csv_encoding(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     heading_row=1)
        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(filename='etk/unit_tests/ground_truth/test_encoding.csv',
                                                     dataset='test_set', encoding='utf-16')]
        expected_docs = [{'Country': 'Algeria', 'Category': 'Crude Oil Production', 'DateTime': '2/28/2018 12:00:00 AM',
                          'Close': '1036.0000', 'Frequency': 'Monthly', 'HistoricalDataSymbol': 'ALGERIACRUOILPRO',
                          'LastUpdate': '3/14/2018 2:17:00 PM',
                          'file_name': 'etk/unit_tests/ground_truth/test_encoding.csv', 'dataset': 'test_set'}]

        self.assertEqual(test_docs, expected_docs)

    def test_dataframe_input(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     heading_row=1,
                                     content_end_row=2,
                                     ends_with_blank_row=False)

        file_path = 'etk/unit_tests/ground_truth/acled_raw_data.csv'
        data = pd.read_csv(file_path)
        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(dataframe=data, dataset='test_set')]

        expected_docs = [{'data_id': 336907, 'iso': 180, 'event_id_cnty': 'DRC11776', 'event_id_no_cnty': 11776,
                          'event_date': '2018-01-13', 'year': 2018, 'time_precision': 1, 'event_type':
                              'Battle-No change of territory',
                          'actor1': 'Military Forces of Democratic Republic of Congo (2001-)',
                          'assoc_actor_1': np.nan, 'inter1': 1, 'actor2': 'ADF: Allied Democratic Forces',
                          'assoc_actor_2': np.nan,
                          'inter2': 2, 'interaction': 12, 'region': 'Central Africa',
                          'country': 'Democratic Republic of Congo',
                          'admin1': 'Nord-Kivu', 'admin2': 'Nord-Kivu', 'admin3': 'Oicha', 'location': 'Oicha',
                          'latitude': 0.7,
                          'longitude': 29.5167, 'geo_precision': 1, 'source': 'Radio Okapi',
                          'source_scale': 'Subnational',
                          'notes': "Presumed FARDC attacked the ADF in the periphery of Oicha on January 13th. Shots were heard "
                                   "in the locality and it is suspected that the FARDC are attacking the 'death triangle' situated in between "
                                   "Mbau, Kamango and Eringeti. The reports are not confirmed by the military.",
                          'fatalities': 0,
                          'timestamp': 1516117305,
                          'dataset': 'test_set'}]

        self.assertEqual(test_docs, expected_docs)

    def test_dataframe_input_fillnan(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     heading_row=1,
                                     content_end_row=3,
                                     ends_with_blank_row=False)

        file_path = 'etk/unit_tests/ground_truth/acled_raw_data.csv'
        data = pd.read_csv(file_path)
        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(dataframe=data, dataset='test_set', fillnan=0)]

        expected_docs = [{'data_id': 336907, 'iso': 180, 'event_id_cnty': 'DRC11776', 'event_id_no_cnty': 11776,
                          'event_date': '2018-01-13', 'year': 2018, 'time_precision': 1, 'event_type':
                              'Battle-No change of territory',
                          'actor1': 'Military Forces of Democratic Republic of Congo (2001-)',
                          'assoc_actor_1': 0, 'inter1': 1, 'actor2': 'ADF: Allied Democratic Forces',
                          'assoc_actor_2': 0,
                          'inter2': 2, 'interaction': 12, 'region': 'Central Africa',
                          'country': 'Democratic Republic of Congo',
                          'admin1': 'Nord-Kivu', 'admin2': 'Nord-Kivu', 'admin3': 'Oicha', 'location': 'Oicha',
                          'latitude': 0.7,
                          'longitude': 29.5167, 'geo_precision': 1, 'source': 'Radio Okapi',
                          'source_scale': 'Subnational',
                          'notes': "Presumed FARDC attacked the ADF in the periphery of Oicha on January 13th. Shots were heard "
                                   "in the locality and it is suspected that the FARDC are attacking the 'death triangle' situated in between "
                                   "Mbau, Kamango and Eringeti. The reports are not confirmed by the military.",
                          'fatalities': 0,
                          'timestamp': 1516117305,
                          'dataset': 'test_set'},
                         {'data_id': 336908, 'iso': 180, 'event_id_cnty': 'DRC11777', 'event_id_no_cnty': 11777,
                          'event_date': '2018-01-13', 'year': 2018, 'time_precision': 1,
                          'event_type': 'Battle-No change of territory',
                          'actor1': 'Military Forces of Democratic Republic of Congo (2001-)', 'assoc_actor_1': 0,
                          'inter1': 1,
                          'actor2': 'ADF: Allied Democratic Forces', 'assoc_actor_2': 0, 'inter2': 2,
                          'interaction': 12,
                          'region': 'Central Africa', 'country': 'Democratic Republic of Congo', 'admin1': 'Nord-Kivu',
                          'admin2': 'Beni', 'admin3': 'Beni', 'location': 'Beni', 'latitude': 0.49658,
                          'longitude': 29.4654,
                          'geo_precision': 1, 'source': 'Reuters; Radio Okapi', 'source_scale': 'Subnational',
                          'notes': 'The FARDC launched, on January 13th, an offensive against the ADF in Beni and Lubero, '
                                   'in response to the recents attacks by the group. Gunfires and explosions were heard '
                                   'in Beni all throughout Saturday (13th).', 'fatalities': 0, 'timestamp': 1516117305,
                          'dataset': 'test_set'}]

        self.assertEqual(test_docs, expected_docs)

    def test_dataframe_input_fillnan_df_string(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     heading_row=1,
                                     content_end_row=3,
                                     ends_with_blank_row=False)

        file_path = 'etk/unit_tests/ground_truth/acled_raw_data.csv'
        data = pd.read_csv(file_path)
        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(dataframe=data, dataset='test_set', fillnan=0, df_string=True)]

        expected_docs = [{'data_id': '336907', 'iso': '180', 'event_id_cnty': 'DRC11776', 'event_id_no_cnty': '11776',
                          'event_date': '2018-01-13', 'year': '2018', 'time_precision': '1', 'event_type':
                              'Battle-No change of territory',
                          'actor1': 'Military Forces of Democratic Republic of Congo (2001-)',
                          'assoc_actor_1': '0', 'inter1': '1', 'actor2': 'ADF: Allied Democratic Forces',
                          'assoc_actor_2': '0',
                          'inter2': '2', 'interaction': '12', 'region': 'Central Africa',
                          'country': 'Democratic Republic of Congo',
                          'admin1': 'Nord-Kivu', 'admin2': 'Nord-Kivu', 'admin3': 'Oicha', 'location': 'Oicha',
                          'latitude': '0.7',
                          'longitude': '29.5167', 'geo_precision': '1', 'source': 'Radio Okapi',
                          'source_scale': 'Subnational',
                          'notes': "Presumed FARDC attacked the ADF in the periphery of Oicha on January 13th. Shots were heard "
                                   "in the locality and it is suspected that the FARDC are attacking the 'death triangle' situated in between "
                                   "Mbau, Kamango and Eringeti. The reports are not confirmed by the military.",
                          'fatalities': '0',
                          'timestamp': '1516117305',
                          'dataset': 'test_set'},
                         {'data_id': '336908', 'iso': '180', 'event_id_cnty': 'DRC11777', 'event_id_no_cnty': '11777',
                          'event_date': '2018-01-13', 'year': '2018', 'time_precision': '1',
                          'event_type': 'Battle-No change of territory',
                          'actor1': 'Military Forces of Democratic Republic of Congo (2001-)', 'assoc_actor_1': '0',
                          'inter1': '1',
                          'actor2': 'ADF: Allied Democratic Forces', 'assoc_actor_2': '0', 'inter2': '2',
                          'interaction': '12',
                          'region': 'Central Africa', 'country': 'Democratic Republic of Congo', 'admin1': 'Nord-Kivu',
                          'admin2': 'Beni', 'admin3': 'Beni', 'location': 'Beni', 'latitude': '0.49658',
                          'longitude': '29.4654',
                          'geo_precision': '1', 'source': 'Reuters; Radio Okapi', 'source_scale': 'Subnational',
                          'notes': 'The FARDC launched, on January 13th, an offensive against the ADF in Beni and Lubero, '
                                   'in response to the recents attacks by the group. Gunfires and explosions were heard '
                                   'in Beni all throughout Saturday (13th).', 'fatalities': '0',
                          'timestamp': '1516117305',
                          'dataset': 'test_set'}]

        self.assertEqual(test_docs, expected_docs)

    def test_dataframe_input_2(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     heading_row=1,
                                     content_start_row=9,
                                     content_end_row=10)

        file_path = './etk/unit_tests/ground_truth/masie_4km_allyears_extent_sqkm.csv'
        data = pd.read_csv(file_path, skiprows=1)
        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(dataframe=data, dataset='test_set')]

        expected_docs = [
            {'yyyyddd': 2006008, ' (0) Northern_Hemisphere': 13536736.84, ' (1) Beaufort_Sea': 1069710.81,
             ' (2) Chukchi_Sea': 966006.16, ' (3) East_Siberian_Sea': 1087102.72,
             ' (4) Laptev_Sea': 897773.37,
             ' (5) Kara_Sea': 927602.17, ' (6) Barents_Sea': 474574.82, ' (7) Greenland_Sea': 590029.18,
             ' (8) Baffin_Bay_Gulf_of_St._Lawrence': 1005790.38, ' (9) Canadian_Archipelago': 852715.31,
             ' (10) Hudson_Bay': 1260779.00, ' (11) Central_Arctic': 3240326.47,
             ' (12) Bering_Sea': 692832.54,
             ' (13) Baltic_Sea': 21327.46, ' (14) Sea_of_Okhotsk': 424563.54,
             ' (15) Yellow_Sea': 14830.45,
             ' (16) Cook_Inlet': 8202.95,
             'dataset': 'test_set'},
            {'yyyyddd': 2006009, ' (0) Northern_Hemisphere': 13536887.64, ' (1) Beaufort_Sea': 1069710.81,
             ' (2) Chukchi_Sea': 966006.16, ' (3) East_Siberian_Sea': 1087102.72,
             ' (4) Laptev_Sea': 897773.37,
             ' (5) Kara_Sea': 927602.17, ' (6) Barents_Sea': 474574.82, ' (7) Greenland_Sea': 590029.18,
             ' (8) Baffin_Bay_Gulf_of_St._Lawrence': 1005790.38, ' (9) Canadian_Archipelago': 852715.31,
             ' (10) Hudson_Bay': 1260779.00, ' (11) Central_Arctic': 3240326.47,
             ' (12) Bering_Sea': 692832.54,
             ' (13) Baltic_Sea': 21478.25, ' (14) Sea_of_Okhotsk': 424563.54,
             ' (15) Yellow_Sea': 14830.45,
             ' (16) Cook_Inlet': 8202.95,
             'dataset': 'test_set'}]

        self.assertEqual(test_docs, expected_docs)

    def test_dataframe_input_string(self) -> None:
        csv_processor = CsvProcessor(etk=etk,
                                     heading_row=1,
                                     content_start_row=10,
                                     content_end_row=11)

        file_path = './etk/unit_tests/ground_truth/masie_4km_allyears_extent_sqkm.csv'
        data = pd.read_csv(file_path, skiprows=1)
        test_docs = [doc.cdr_document for doc in
                     csv_processor.tabular_extractor(dataframe=data, dataset='test_set', df_string=True)]

        expected_docs = [
            {'yyyyddd': '2006009', ' (0) Northern_Hemisphere': '13536887.64', ' (1) Beaufort_Sea': '1069710.81',
             ' (2) Chukchi_Sea': '966006.16', ' (3) East_Siberian_Sea': '1087102.72',
             ' (4) Laptev_Sea': '897773.37',
             ' (5) Kara_Sea': '927602.17', ' (6) Barents_Sea': '474574.82', ' (7) Greenland_Sea': '590029.18',
             ' (8) Baffin_Bay_Gulf_of_St._Lawrence': '1005790.38', ' (9) Canadian_Archipelago': '852715.31',
             ' (10) Hudson_Bay': '1260779.0', ' (11) Central_Arctic': '3240326.47',
             ' (12) Bering_Sea': '692832.54',
             ' (13) Baltic_Sea': '21478.25', ' (14) Sea_of_Okhotsk': '424563.54',
             ' (15) Yellow_Sea': '14830.45',
             ' (16) Cook_Inlet': '8202.95',
             'dataset': 'test_set'},
            {'yyyyddd': '2006010', ' (0) Northern_Hemisphere': '13505426.35', ' (1) Beaufort_Sea': '1069710.81',
             ' (2) Chukchi_Sea': '966006.16', ' (3) East_Siberian_Sea': '1087102.72',
             ' (4) Laptev_Sea': '897773.37',
             ' (5) Kara_Sea': '933999.29', ' (6) Barents_Sea': '448185.27', ' (7) Greenland_Sea': '588279.64',
             ' (8) Baffin_Bay_Gulf_of_St._Lawrence': '1016857.87', ' (9) Canadian_Archipelago': '852715.31',
             ' (10) Hudson_Bay': '1260779.0', ' (11) Central_Arctic': '3217380.82',
             ' (12) Bering_Sea': '705348.17',
             ' (13) Baltic_Sea': '21493.81', ' (14) Sea_of_Okhotsk': '414191.19',
             ' (15) Yellow_Sea': '14830.45',
             ' (16) Cook_Inlet': '8202.95',
             'dataset': 'test_set'}]

        self.assertEqual(test_docs, expected_docs)


if __name__ == '__main__':
    unittest.main()