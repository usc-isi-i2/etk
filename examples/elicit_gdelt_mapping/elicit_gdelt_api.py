import json
from optparse import OptionParser

event_fields = ['event1', 'event1-arguments', 'event2 (has_topic of event1)', 'event2-arguments',
                'event3 (has_topic of event2)', 'event3-arguments']


class GdeltMapping(object):
    def __init__(self, elicit_gdelt_mapping):
        self.elicit_gdelt_mapping = elicit_gdelt_mapping

    def gdelt_api(self, cameo_code):
        if cameo_code not in self.elicit_gdelt_mapping:
            print('cameo_code: {} not mapped and does not exist in the excel'.format(cameo_code))
            return None
        print(cameo_code)
        cameo_code_details = self.elicit_gdelt_mapping[cameo_code]
        result = dict()
        event1 = dict()
        event2 = dict()
        event3 = dict()

        # get event1 details
        event1_types = cameo_code_details['event1']
        event1['type'] = list()
        if not isinstance(event1_types, list):
            event1_types = [event1_types]
        for e1_type in event1_types:
            event1['type'].append(self.get_ontology_entity_details(e1_type))
        e1_arguments = cameo_code_details['event1-arguments']
        if not isinstance(e1_arguments, list):
            e1_arguments = [e1_arguments]
        for e1_argument in e1_arguments:
            e1_d1 = self.get_ontology_entity_details(e1_argument)
            if e1_d1:
                event1[e1_d1[1]] = e1_d1[0]

        # get event2 details
        event2_types = cameo_code_details['event2 (has_topic of event1)']
        event2['type'] = list()
        if not isinstance(event2_types, list):
            event2_types = [event2_types]
        for e2_type in event2_types:
            event2['type'].append(self.get_ontology_entity_details(e2_type))
        # if event2_type:
        #     event2['type'] = event2_type
        e2_arguments = cameo_code_details['event2-arguments']
        if not isinstance(e2_arguments, list):
            e2_arguments = [e2_arguments]
        for e2_argument in e2_arguments:
            e2_d1 = self.get_ontology_entity_details(e2_argument)
            if len(e2_d1) > 1:
                event2[e2_d1[1]] = e2_d1[0]

        # get event3 details
        event3_type = self.get_ontology_entity_details(cameo_code_details['event3 (has_topic of event2)'])[0]
        if event3_type:
            event3['type'] = event3_type
        e3_arguments = cameo_code_details['event3-arguments']
        if not isinstance(e3_arguments, list):
            e3_arguments = [e3_arguments]
        for e3_argument in e3_arguments:
            e3_d1 = self.get_ontology_entity_details(e3_argument)
            if len(e3_d1) > 1:
                event3[e3_d1[1]] = e3_d1[0]

        result['event1'] = event1
        result['event2'] = event2
        result['event3'] = event3
        return result

    @staticmethod
    def get_ontology_entity_details(ontology_entity):
        print(ontology_entity)
        spaces = ontology_entity.split(' ')
        spaces = [space.split(':')[1] if ':' in space else space for space in spaces]
        return spaces

    def get_all_cameo_codes(self):
        result = dict()
        for cameo_code in self.elicit_gdelt_mapping.keys():
            result[cameo_code] = self.gdelt_api(cameo_code)
        return result


if __name__ == "__main__":
    parser = OptionParser()

    (c_options, args) = parser.parse_args()
    gdelt_mapping_path = args[0]
    gdelt_api_obj = GdeltMapping(json.load(open(gdelt_mapping_path)))
    # print(gdelt_api_obj.gdelt_api("010"))
    print(gdelt_api_obj.get_all_cameo_codes())
