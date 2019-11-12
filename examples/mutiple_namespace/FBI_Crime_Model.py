import json
from etk.etk import ETK
from etk.extractors.excel_extractor import ExcelExtractor
from etk.knowledge_graph import KGSchema, URI, Literal
from etk.etk_module import ETKModule
from etk.wikidata.__init__ import create_custom_prefix
from etk.wikidata.entity import WDProperty, WDItem
from etk.wikidata.value import Datatype, Item, QuantityValue
from etk.wikidata.statement import WDReference


class FBICrimeModel():
    def __init__(self):

        self.value_dict = {
            'value': '$col,$row',
            'county': '$B,$row',
            'category': '$col,$6',
            'from_row': '$row',
            'from_col': '$col'
        }

        self.county_QNode = dict()

        # hashmap for states and their abbrviate
        self.state_abbr = {'alabama': 'al'}

    def add_value(self, item, key, value, unit, reference):

        # manually define kv pairs dict
        kv_dict = {'Violent_crime': 'C3001', 'Murder_and_nonnegligent_manslaughter': 'C3002',
                   'Rape_(revised_definition)1': 'C3003', 'Rape_(revised_definition)': 'C3003',
                   'Rape_(legacy_definition)2': 'C3004', 'Rape_(legacy_definition)': 'C3004', 'Robbery': 'C3005',
                   'Aggravated_assault': 'C3006', 'Property_crime': 'C3007', 'Burglary': 'C3008',
                   'Larceny-_theft': 'C3009', 'Motor_vehicle_theft': 'C3010', 'Arson3': 'C3011', 'Arson': 'C3011'}

        if key not in kv_dict:
            raise Exception(key + ' is not implemented')

        # add statement
        s = item.add_statement(kv_dict[key], QuantityValue(value, unit=unit, namespace='cm'), namespace='cm')
        s.add_reference(reference)

    def extract_data(self, state=None):

        # Initiate Excel Extractor
        ee = ExcelExtractor()
        # read file
        file_path = state + '.xls'
        print('Extracting crime data for ' + state)

        # extract data from excel files
        extractions = ee.extract(file_name=file_path,
                                 sheet_name='16tbl08al',
                                 region=['B,7', 'N,100'],
                                 variables=self.value_dict)
        # build dictionary
        county_data = dict()
        for e in extractions:
            if len(e['county']) > 0:

                # build county dictionary
                if e['county'] not in county_data:
                    county_data[e['county']] = dict()

                # add extracted data
                if e['value'] is not '' and isinstance(e['value'], int):
                    e['category'] = e['category'].replace('\n', '_').replace(' ', '_')
                    county_data[e['county']][e['category']] = e['value']
        print('\n\nExtraction completed!')
        return county_data

    def model_data(self, crime_data, file_path, format='ttl'):

        # initialize
        kg_schema = KGSchema()
        kg_schema.add_schema('@prefix : <http://isi.edu/> .', 'ttl')
        etk = ETK(kg_schema=kg_schema, modules=ETKModule)
        doc = etk.create_document({}, doc_id="http://isi.edu/default-ns/projects")

        # bind prefixes
        custom_dict = {'cm': 'https://w3id.org/satellite/cm'}
        doc = create_custom_prefix(doc, custom_dict)

        # first we add properties and entities
        # Define Qnode for properties related to crime.
        q = WDItem('D1001', namespace='cm')
        q.add_label('Wikidata property related to crime', lang='en')
        q.add_statement('P279', Item('Q22984475'))
        q.add_statement('P1269', Item('Q83267'))
        doc.kg.add_subject(q)

        # violent crime offenses
        p = WDProperty('C3001', Datatype.QuantityValue, namespace='cm')
        p.add_label('violent crime offenses', lang='en')
        p.add_description(
            "number of violent crime offenses reported by the sheriff's office or county police department", lang='en')
        p.add_statement('P31', Item('D1001', namespace='cm'))
        p.add_statement('P1629', Item('Q1520311'))
        doc.kg.add_subject(p)

        # murder and non - negligent manslaughter
        p = WDProperty('C3002', Datatype.QuantityValue, namespace='cm')
        p.add_label('murder and non-negligent manslaughter', lang='en')
        p.add_description(
            "number of murder and non-negligent manslaughter offenses reported by the sheriff's office or county police department",
            lang='en')
        p.add_statement('P31', Item('D1001', namespace='cm'))
        p.add_statement('P1629', Item('Q1295558'))
        p.add_statement('P1629', Item('Q132821'))
        doc.kg.add_subject(p)

        # Rape(revised definition)
        p = WDProperty('C3003', Datatype.QuantityValue, namespace='cm')
        p.add_label('Rape (revised definition)', lang='en')
        p.add_description(
            "number of rapes (revised definition) reported by the sheriff's office or county police department",
            lang='en')
        p.add_statement('P31', Item('D1001', namespace='cm'))
        p.add_statement('P1629', Item('Q47092'))
        doc.kg.add_subject(p)

        # Rape(legacy definition)
        p = WDProperty('C3004', Datatype.QuantityValue, namespace='cm')
        p.add_label('Rape (legacy definition)', lang='en')
        p.add_description(
            "number of rapes (legacy definition) reported by the sheriff's office or county police department",
            lang='en')
        p.add_statement('P31', Item('D1001', namespace='cm'))
        p.add_statement('P1629', Item('Q47092'))
        doc.kg.add_subject(p)

        # robbery
        p = WDProperty('C3005', Datatype.QuantityValue, namespace='cm')
        p.add_label('Robbery', lang='en')
        p.add_description("number of roberies reported by the sheriff's office or county police department", lang='en')
        p.add_statement('P31', Item('D1001', namespace='cm'))
        p.add_statement('P1629', Item('Q53706'))
        doc.kg.add_subject(p)

        # aggravated assault
        p = WDProperty('C3006', Datatype.QuantityValue, namespace='cm')
        p.add_label('Aggravated assault', lang='en')
        p.add_description("number of aggravated assaults reported by the sheriff's office or county police department",
                          lang='en')
        p.add_statement('P31', Item('D1001', namespace='cm'))
        p.add_statement('P1629', Item('Q365680'))
        p.add_statement('P1629', Item('Q81672'))
        doc.kg.add_subject(p)

        # property crime
        p = WDProperty('C3007', Datatype.QuantityValue, namespace='cm')
        p.add_label('Property crime', lang='en')
        p.add_description("number of property crimes reported by the sheriff's office or county police department",
                          lang='en')
        p.add_statement('P31', Item('D1001', namespace='cm'))
        p.add_statement('P1629', Item('Q857984'))
        doc.kg.add_subject(p)

        # burglary
        p = WDProperty('C3008', Datatype.QuantityValue, namespace='cm')
        p.add_label('Burglary', lang='en')
        p.add_description("number of Burglaries reported by the sheriff's office or county police department",
                          lang='en')
        p.add_statement('P31', Item('D1001', namespace='cm'))
        p.add_statement('P1629', Item('Q329425'))
        doc.kg.add_subject(p)

        # larceny - theft
        p = WDProperty('C3009', Datatype.QuantityValue, namespace='cm')
        p.add_label('Larceny-theft', lang='en')
        p.add_description("number of Larceny-theft reported by the sheriff's office or county police department",
                          lang='en')
        p.add_statement('P31', Item('D1001', namespace='cm'))
        p.add_statement('P1629', Item('Q2485381'))
        p.add_statement('P1629', Item('Q2727213'))
        doc.kg.add_subject(p)

        # motor vehicle theft
        p = WDProperty('C3010', Datatype.QuantityValue, namespace='cm')
        p.add_label('Motor vehicle theft', lang='en')
        p.add_description("number of Motor vehicle thefts reported by the sheriff's office or county police department",
                          lang='en')
        p.add_statement('P31', Item('D1001', namespace='cm'))
        p.add_statement('P1629', Item('Q548007'))
        p.add_statement('P1629', Item('Q2727213'))
        doc.kg.add_subject(p)

        # arson
        p = WDProperty('C3011', Datatype.QuantityValue, namespace='cm')
        p.add_label('Arson', lang='en')
        p.add_description("number of arsons reported by the sheriff's office or county police department", lang='en')
        p.add_statement('P31', Item('D1001', namespace='cm'))
        p.add_statement('P1629', Item('Q327541'))
        doc.kg.add_subject(p)

        # Offenses are reported for a period of type,
        # so the quantity needs to be represented in units such as offenses / year
        unit = WDItem('D1002', namespace='cm')
        unit.add_label('offenses per year', lang='en')
        unit.add_statement('P31', Item('Q47574'))
        unit.add_statement('P1629', Item('Q83267'))
        # doc.kg.add_subject(unit)

        # we begin to model data extracted
        # add reference, data source
        download_url = 'https://ucr.fbi.gov/crime-in-the-u.s/2016/crime-in-the-u.s.-2016/tables/table-8' \
                       '/table-8-state-cuts/alabama.xls'
        reference = WDReference()
        reference.add_property(URI('P248'), URI('wd:Q8333'))
        reference.add_property(URI('P854'), Literal(download_url))

        for county in crime_data.keys():

            # county entity
            QNode = self.get_QNode(county)
            if not QNode:
                continue
            q = WDItem(QNode)

            # add value for each property
            for property in crime_data[county]:
                self.add_value(q, property, crime_data[county][property], unit, reference)
            # add the entity to kg
            doc.kg.add_subject(q)
        print('\n\nModeling completed!\n\n')
        f = open(file_path, 'w')
        f.write(doc.kg.serialize(format))
        print('Serialization completed!')

    def get_QNode(self, county, state='alabama'):

        # read county_QNode.json to build dictionary
        if len(self.county_QNode) == 0:
            f = open('county_qnode.json', 'r')
            self.county_QNode = json.loads(f.read())
            f.close()
        name = county.lower().replace(' ', '-')
        temp_state = state.lower().replace(' ', '-')

        # map state to its abbreviate
        if temp_state in self.state_abbr:
            abbr = self.state_abbr[temp_state]

            # find Qnode
            county1 = name + '-county_' + abbr
            county2 = name + '_' + abbr
            if county1 in self.county_QNode:
                return self.county_QNode[county1]
            if county2 in self.county_QNode:
                return self.county_QNode[county2]
        return None


if __name__ == "__main__":
    model = FBICrimeModel()

    res = model.extract_data('alabama')
    model.model_data(res, 'alabama.ttl')