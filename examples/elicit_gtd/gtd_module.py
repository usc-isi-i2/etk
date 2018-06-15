import os
import sys, json
from typing import List

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.etk_module import ETKModule
from etk.csv_processor import CsvProcessor
from etk.extractors.decoding_value_extractor import DecodingValueExtractor
from etk.document_selector import DefaultDocumentSelector
from etk.document import Document
from etk.knowledge_graph_schema import KGSchema
from etk.utilities import Utility
from etk.extractors.date_extractor import DateExtractor

place_fields = ['country_txt', 'region_txt', 'provstate', 'city', 'latitude', 'longitude', 'location']
place_field_mapping = {
    "country_txt": "country",
    "region_txt": "region",
    "provstate": "state",
    "city": "city",
    "latitude": "latitude",
    "longitude": "longitude",
    "location": "title"
}
inclusion_criteria_1 = "Political, Economics, Religious or Social Goal"
inclusion_criteria_2 = "Intention to coerce, Intimidate or Publicize to larger audiences"
inclusion_criteria_3 = "Outside International Humanitarian Law"
attack_type_fields = ["attacktype1_txt", "attacktype2_txt", "attacktype3_txt"]


class GTDModule(ETKModule):
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.date_extractor = DateExtractor(self.etk, 'gtd_date_parser')

    def process_document(self, doc: Document) -> List[Document]:
        nested_docs = list()

        json_doc = doc.cdr_document
        doc.doc_id = Utility.create_doc_id_from_json(json_doc)
        doc.cdr_document['uri'] = doc.doc_id
        doc.kg.add_value("type", value="Event")
        doc.kg.add_value("type", value="Act of Terrorism")

        # Add event_date to the KG
        extracted_dates = self.date_extractor.extract('{}-{}-{}'.format(json_doc.get('iyear'),
                                                                        json_doc.get('imonth'), json_doc.get('iday')))
        if len(extracted_dates) > 0:
            doc.kg.add_value("event_date", value=extracted_dates)
        else:
            # no proper date mentioned in the event, try the approximate date
            approximate_date_txt = json_doc.get("approxdate")
            extracted_approx_dates = self.date_extractor.extract(approximate_date_txt)
            if len(extracted_approx_dates) > 0:
                doc.kg.add_value("event_date", value=extracted_approx_dates)

        # summary, aka description only available for incident after 1997
        doc.kg.add_value("description", json_path="$.summary")

        # add inclusion criteria: why is this incident regarded as a terrorist incident
        # TODO: ADD this to master_config
        crit1 = json_doc.get('crit1', 0)
        if crit1 == 1:
            doc.kg.add_value("inclusion_criteria", value=inclusion_criteria_1)

        crit2 = json_doc.get('crit2', 0)
        if crit2 == 1:
            doc.kg.add_value("inclusion_criteria", value=inclusion_criteria_2)

        crit3 = json_doc.get('crit3', 0)
        if crit3 == 1:
            doc.kg.add_value("inclusion_criteria", value=inclusion_criteria_3)

        # add related events to KG
        # TODO: ADD this to master_config
        related_event_ids = json_doc.get('related', '').split(',')
        if related_event_ids:
            doc.kg.add_value("related_events", value=related_event_ids)

        # add attack information, on second thoughts, this qualifies as event type
        for attack_type_field in attack_type_fields:
            doc.kg.add_value("type", value=json_doc.get(attack_type_field, ''))

        # TODO check the following 2
        if json_doc.get("suicide", 0) == 1:
            doc.kg.add_value("type", value='suicide')

        if json_doc.get("success", 0) == 1:
            doc.kg.add_value("type", value='success')

        # create nested objects for places
        place_object = dict()
        for place_field in place_fields:
            place_object[place_field] = json_doc.get(place_field)
        place_object["dataset"] = "gtd_place"

        place_doc_id = '{}_place'.format(doc.doc_id)
        place_object['uri'] = place_doc_id
        place_doc = etk.create_document(place_object)
        place_doc.doc_id = place_doc_id

        doc.kg.add_value("place", value=place_doc.doc_id)
        nested_docs.append(place_doc)

        # create victim objects, there can be upto 3
        if json_doc.get('targtype1_txt', '').strip():
            victim1_object = dict()
            victim1_object['dataset'] = 'gtd_victim'
            victim1_object['victim_type'] = list()
            victim1_object['victim_type'].append(json_doc.get('targtype1_txt'))
            if json_doc.get('targsubtype1_txt', ''):
                victim1_object['victim_type'].append(json_doc.get('targsubtype1_txt'))
            victim1_object['victim_corp'] = json_doc.get('corp1', '')
            victim1_object['victim_target'] = json_doc.get('target1', '')
            victim1_object['victim_nationality'] = json_doc.get('natlty1_txt', '')
            victim1_doc_id = '{}_victim1'.format(doc.doc_id)
            victim1_object['uri'] = victim1_doc_id
            victim1_doc = etk.create_document(victim1_object)
            victim1_doc.doc_id = victim1_doc_id
            doc.kg.add_value('victim', value=victim1_doc.doc_id)
            nested_docs.append(victim1_doc)

        if json_doc.get('targtype2_txt', '').strip():
            victim2_object = dict()
            victim2_object['dataset'] = 'gtd_victim'
            victim2_object['victim_type'] = list()
            victim2_object['victim_type'].append(json_doc.get('targtype2_txt'))
            if json_doc.get('targsubtype2_txt', ''):
                victim2_object['victim_type'].append(json_doc.get('targsubtype2_txt'))
            victim2_object['victim_corp'] = json_doc.get('corp2', '')
            victim2_object['victim_target'] = json_doc.get('target2', '')
            victim2_object['victim_nationality'] = json_doc.get('natlty2_txt', '')
            victim2_doc_id = '{}_victim2'.format(doc.doc_id)
            victim2_object['uri'] = victim2_doc_id
            victim2_doc = etk.create_document(victim2_object)
            victim2_doc.doc_id = victim2_doc_id
            doc.kg.add_value('victim', value=victim2_doc.doc_id)
            nested_docs.append(victim2_doc)

        if json_doc.get('targtype3_txt', '').strip():
            victim3_object = dict()
            victim3_object['dataset'] = 'gtd_victim'
            victim3_object['victim_type'] = list()
            victim3_object['victim_type'].append(json_doc.get('targtype3_txt'))
            if json_doc.get('targsubtype3_txt', ''):
                victim3_object['victim_type'].append(json_doc.get('targsubtype3_txt'))
            victim3_object['victim_corp'] = json_doc.get('corp3', '')
            victim3_object['victim_target'] = json_doc.get('target3', '')
            victim3_object['victim_nationality'] = json_doc.get('natlty3_txt', '')
            victim3_doc_id = '{}_victim3'.format(doc.doc_id)
            victim3_object['uri'] = victim3_doc_id
            victim3_doc = etk.create_document(victim3_object)
            victim3_doc.doc_id = victim3_doc_id
            doc.kg.add_value('victim', value=victim3_doc.doc_id)
            nested_docs.append(victim3_doc)

        # create actor/perpetrators objects
        if json_doc.get('gname', '').strip():
            actor1_object = dict()
            actor1_object['dataset'] = 'gtd_actor'
            actor1_object['actor_group'] = list()
            actor1_object['actor_group'].append(json_doc.get('gname'))
            if json_doc.get('gsubname', ''):
                actor1_object['actor_group'].append(json_doc.get('gsubname'))

            actor1_doc_id = '{}_actor1'.format(doc.doc_id)
            actor1_object['uri'] = actor1_doc_id
            actor1_doc = etk.create_document(actor1_object)
            actor1_doc.doc_id = actor1_doc_id
            doc.kg.add_value('actor', value=actor1_doc.doc_id)
            nested_docs.append(actor1_doc)

        if json_doc.get('gname2', '').strip():
            actor2_object = dict()
            actor2_object['dataset'] = 'gtd_actor'
            actor2_object['actor_group'] = list()
            actor2_object['actor_group'].append(json_doc.get('gname2'))
            if json_doc.get('gsubname2', ''):
                actor2_object['actor_group'].append(json_doc.get('gsubname2'))
            actor2_doc_id = '{}_actor2'.format(doc.doc_id)
            actor2_object['uri'] = actor2_doc_id
            actor2_doc = etk.create_document(actor2_object)
            actor2_doc.doc_id = actor2_doc_id
            doc.kg.add_value('actor', value=actor2_doc.doc_id)
            nested_docs.append(actor2_doc)

        if json_doc.get('gname3', '').strip():
            actor3_object = dict()
            actor3_object['dataset'] = 'gtd_actor'
            actor3_object['actor_group'] = list()
            actor3_object['actor_group'].append(json_doc.get('gname3'))
            if json_doc.get('gsubname3', ''):
                actor3_object['actor_group'].append(json_doc.get('gsubname3'))
            actor3_doc_id = '{}_actor3'.format(doc.doc_id)
            actor3_object['uri'] = actor3_doc_id
            actor3_doc = etk.create_document(actor3_object)
            actor3_doc.doc_id = actor3_doc_id
            doc.kg.add_value('actor', value=actor3_doc.doc_id)
            nested_docs.append(actor3_doc)

        # create weapon objects, upto 4
        if json_doc.get('weaptype1_txt', '').strip():
            weapon1_object = dict()
            weapon1_object['dataset'] = 'gtd_weapon'
            weapon1_object['weapon_title'] = json_doc.get('weapdetail', '')
            weapon1_object['weapon_type'] = list()
            weapon1_object['weapon_type'].append(json_doc.get('weaptype1_txt'))
            if json_doc.get('weapsubtype1_txt', ''):
                weapon1_object['weapon_type'].append(json_doc.get('weapsubtype1_txt'))
            weapon1_doc_id = '{}_weapons1'.format(doc.doc_id)
            weapon1_object['uri'] = weapon1_doc_id
            weapon1_doc = etk.create_document(weapon1_object)
            weapon1_doc.doc_id = weapon1_doc_id
            doc.kg.add_value('weapons', weapon1_doc.doc_id)
            nested_docs.append(weapon1_doc)

        if json_doc.get('weaptype2_txt', '').strip():
            weapon2_object = dict()
            weapon2_object['dataset'] = 'gtd_weapon'
            weapon2_object['weapon_title'] = json_doc.get('weapdetail', '')
            weapon2_object['weapon_type'] = list()
            weapon2_object['weapon_type'].append(json_doc.get('weaptype2_txt'))
            if json_doc.get('weapsubtype2_txt', ''):
                weapon2_object['weapon_type'].append(json_doc.get('weapsubtype2_txt'))
            weapon2_doc_id = '{}_weapons2'.format(doc.doc_id)
            weapon2_object['uri'] = weapon2_doc_id
            weapon2_doc = etk.create_document(weapon2_object)
            weapon2_doc.doc_id = weapon2_doc_id
            doc.kg.add_value('weapons', weapon2_doc.doc_id)
            nested_docs.append(weapon2_doc)

        if json_doc.get('weaptype3_txt', '').strip():
            weapon3_object = dict()
            weapon3_object['dataset'] = 'gtd_weapon'
            weapon3_object['weapon_title'] = json_doc.get('weapdetail', '')
            weapon3_object['weapon_type'] = list()
            weapon3_object['weapon_type'].append(json_doc.get('weaptype3_txt'))
            if json_doc.get('weapsubtype3_txt', ''):
                weapon3_object['weapon_type'].append(json_doc.get('weapsubtype3_txt'))
            weapon3_doc_id = '{}_weapons3'.format(doc.doc_id)
            weapon3_object['uri'] = weapon3_doc_id
            weapon3_doc = etk.create_document(weapon3_object)
            weapon3_doc.doc_id = weapon3_doc_id
            doc.kg.add_value('weapons', weapon3_doc.doc_id)
            nested_docs.append(weapon3_doc)

        if json_doc.get('weaptype4_txt', '').strip():
            weapon4_object = dict()
            weapon4_object['dataset'] = 'gtd_weapon'
            weapon4_object['weapon_title'] = json_doc.get('weapdetail', '')
            weapon4_object['weapon_type'] = list()
            weapon4_object['weapon_type'].append(json_doc.get('weaptype4_txt'))
            if json_doc.get('weapsubtype4_txt', ''):
                weapon4_object['weapon_type'].append(json_doc.get('weapsubtype4_txt'))
            weapon4_doc_id = '{}_weapons4'.format(doc.doc_id)
            weapon4_object['uri'] = weapon4_doc_id
            weapon4_doc = etk.create_document(weapon4_object)
            weapon4_doc.doc_id = weapon4_doc_id
            doc.kg.add_value('weapons', weapon4_doc.doc_id)
            nested_docs.append(weapon4_doc)

        return nested_docs
    def document_selector(self, doc) -> bool:
        return doc.cdr_document.get("dataset") == "gtd"


class GTDPlaceModule(ETKModule):
    """
        ETK module to process GTD Place documents
        """

    def __init__(self, etk):
        ETKModule.__init__(self, etk)

    def document_selector(self, doc) -> bool:
        return doc.cdr_document.get("dataset") == "gtd_place"

    def process_document(self, doc: Document) -> List[Document]:
        doc.kg.add_value("type", value="Place")
        for place_field in place_fields:
            doc.kg.add_value(place_field_mapping[place_field], json_path='$.{}'.format(place_field))

        return list()


class GTDVictimModule(ETKModule):
    """
        ETK module to process GTD Victim documents
        """

    def __init__(self, etk):
        ETKModule.__init__(self, etk)

    def document_selector(self, doc) -> bool:
        return doc.cdr_document.get("dataset") == "gtd_victim"

    def process_document(self, doc: Document) -> List[Document]:
        doc.kg.add_value("type", value="Victim")
        doc.kg.add_value("type", json_path="$.victim_type[*]")
        doc.kg.add_value("name", json_path="$.victim_corp")
        doc.kg.add_value("title", json_path="$.victim_target")
        doc.kg.add_value("nationality", json_path="$.victim_nationality")

        return list()


class GTDActorModule(ETKModule):
    """
        ETK module to process GTD Actor/Perpetrator documents
        """

    def __init__(self, etk):
        ETKModule.__init__(self, etk)

    def document_selector(self, doc) -> bool:
        return doc.cdr_document.get("dataset") == "gtd_actor"

    def process_document(self, doc: Document) -> List[Document]:
        doc.kg.add_value("type", value=["Actor", "Perpetrator"])
        doc.kg.add_value("name", json_path="$.actor_group[*]")
        return list()


class GTDWeaponsModule(ETKModule):
    """
        ETK module to process GTD Weapon documents
        """

    def __init__(self, etk):
        ETKModule.__init__(self, etk)

    def document_selector(self, doc) -> bool:
        return doc.cdr_document.get("dataset") == "gtd_weapon"

    def process_document(self, doc: Document) -> List[Document]:
        doc.kg.add_value("type", value="Weapon")
        doc.kg.add_value("title", json_path="$.weapon_title")
        doc.kg.add_value("type", json_path="$.weapon_type[*]")
        return list()


if __name__ == "__main__":

    # Tell ETK the schema of the fields in the KG, the DIG master_config can be used as the schema.
    kg_schema = KGSchema(json.load(open('master_config.json')))

    # Instantiate ETK, with the two processing modules and the schema.
    etk = ETK(modules=[GTDModule], kg_schema=kg_schema)

    # Create a CSV processor to create documents for the relevant rows in the Excel sheet
    cp = CsvProcessor(etk=etk, heading_row=1)

    with open("gtd.jl", "w") as f:
        # Iterate over all the rows in the spredsheet
        for doc in cp.tabular_extractor(filename="globalterrorismdb_0617dist-nigeria.csv", dataset='gtd'):
            print(json.dumps(doc.value, indent=2))
            exit(0)
            for result in etk.process_ems(doc):
                print(result.cdr_document["knowledge_graph"])
                f.write(json.dumps(result.cdr_document) + "\n")
