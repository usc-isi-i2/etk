import json
import os
from etk.etk import ETK
from etk.knowledge_graph.schema import KGSchema
from etk.etk_module import ETKModule
from etk.knowledge_graph.node import URI, BNode, Literal, LiteralType
from etk.knowledge_graph.subject import Subject


class ExampleETKModule(ETKModule):
    """
    Abstract class for extraction module
    """

    def __init__(self, etk):
        ETKModule.__init__(self, etk)

        # Load mapping file, maps full country names to abbrevation codes
        abbv_file_path = 'resources/ctr_code.json'
        self.abbv_dict = self.abbv_reader(abbv_file_path)

    def abbv_reader(self, path):
        f = open(path, 'r')
        abbv_dict = json.loads(f.read())
        f.close()
        return abbv_dict

    def process_document(self, doc):
        """
        Add your code for processing the document
        """

        # Bind ontologies
        doc.kg.bind('cameo', 'http://ontology.causeex.com/cameo/CountryCodeOntology/')
        doc.kg.bind('cco', 'http://www.ontologyrepository.com/CommonCoreOntologies/')
        doc.kg.bind('icm', 'http://ontology.causeex.com/ontology/odps/ICM#')
        doc.kg.bind('meas', 'http://ontology.causeex.com/ontology/odps/TimeSeriesAndMeasurements#')
        doc.kg.bind('isi', 'http://elicit.isi.edu/data/')
        doc.kg.bind('actor', 'http://ontology.causeex.com/ontology/odps/Actor#')
        doc.kg.bind('eth', 'http://www.ontologylibrary.mil/OperationalEnvironment/Mid/OEEthnicityOntology#')

        # replace country names with their abbrevation codes to generate uris
        countries = doc.select_segments("countries")[0].value.keys()
        for country in countries:
            if country != 'world':
                country_uri = country
                if country.lower() in self.abbv_dict:
                    country_uri = 'CAMEO' + self.abbv_dict[country.lower()]

                # Replace some special characters in the dataset
                if '\'' in country:
                    old_ctr = country
                    country = country.replace('\'', '')
                    doc.cdr_document['countries'][country] = doc.cdr_document['countries'].pop(old_ctr)

                # We use json path to extract data from document objects
                area_path = 'countries.' + country + '.data.geography.area.total.value'  # json path of area data of a country
                area_value = doc.select_segments(area_path)
                area_unit_path = 'countries.' + country + '.data.geography.area.total.units'
                measure_unit = doc.select_segments(area_unit_path)

                pop_path = 'countries.' + country + '.data.people.population.total'
                pop_value = doc.select_segments(pop_path)

                age_struct_path = 'countries.' + country + '.data.people.age_structure'
                age_struct = doc.select_segments(age_struct_path)
                age_struct_values = list()
                age_struct_values_percent = list()
                if age_struct:
                    for key in age_struct[0].value:
                        if key != 'date':
                            age_struct_values.append(key)
                            age_struct_values_percent.append(age_struct[0].value[key]['percent'])

                ethnicity_path = 'countries.' + country + '.data.people.ethnic_groups.ethnicity'
                ethnicity = doc.select_segments(ethnicity_path)
                ethnicity_values = list()
                if ethnicity:
                    for key in ethnicity[0].value:
                        if key.get('name'):
                            ethnicity_values.append(key['name'].replace('<', ' ').replace('"', ' ').replace(' ', '_'))

                sex_ratio_path = 'countries.' + country + '.data.people.ethnic_groups.sex_ratio.by_age'
                sex_ratio = doc.select_segments(sex_ratio_path)
                sex_ratio_key_values = list()
                sex_ratio_values = list()
                if sex_ratio:
                    for key in sex_ratio[0].value:
                        if key != 'date':
                            sex_ratio_key_values.append(key)
                            sex_ratio_values.append(sex_ratio[0].value[key]['value'])

                religion_path = 'countries.' + country + '.data.people.religions.religion'
                religion = doc.select_segments(religion_path)
                religion_values = list()
                religion_percent_values = list()
                if religion:
                    for key in religion[0].value:
                        if 'name' in key and 'percent' in key:
                            religion_values.append(key['name'])
                            religion_percent_values.append(key['percent'])

                people_path = 'countries.' + country + '.data.people'
                people = doc.select_segments(people_path)
                mor_dict = {}
                if people:
                    for key in people[0].value:
                        if 'infant_mortality_rate' in key:
                            for i in people[0].value['infant_mortality_rate']:
                                if i != 'global_rank' and i != 'date':
                                    mor_dict[key] = people[0].value['infant_mortality_rate'][i]['value']

                gdp_path = 'countries.' + country + '.data.economy.gdp.purchasing_power_parity.annual_values'
                gdp_value = doc.select_segments(gdp_path)
                labor_force_path = 'countries.' + country + '.data.economy.labor_force.total_size.total_people'
                labor_force_value = doc.select_segments(labor_force_path)
                unem_rate_path = 'countries.' + country + '.data.economy.unemployment_rate.annual_values'
                unem_rate_value = doc.select_segments(unem_rate_path)
                military_path = 'countries.' + country + '.data.military_and_security.expenditures.annual_values'
                military_value = doc.select_segments(military_path)

                # From here, we create triples using the data above

                # Create a subject first
                country_subject = Subject(URI('isi:country-' + country))
                # Then we may add multiple properties to the subject we created
                country_subject.add_property(URI('rdf:type'), URI('cco:country'))
                country_subject.add_property(URI('rdf:type'), URI('cameo:' + country_uri))
                country_subject.add_property(URI('rdfs:label'), Literal(country))

                actor_subject = Subject(URI('isi:country-' + country + '-actor'))
                actor_subject.add_property(URI('rdf:type'), URI('actor:Actor'))

                # Add the actor_subject as a property of country_subject
                country_subject.add_property(URI('icm:has_backing_entity'), actor_subject)

                # Add triples for area data
                rv_subject = Subject(URI('isi:' + country + '-area'))
                rv_subject.add_property(URI('rdf:type'), URI('meas:ReportedValue'))
                country_subject.add_property(URI('meas:has_reported_value'), rv_subject)
                if area_value:
                    # Add decimal type to this Literal
                    rv_subject.add_property(URI('cco:has_decimal_value'),
                                            Literal(str(area_value[0].value), type_=LiteralType.decimal))
                    rp_subject = Subject(URI('isi:area-reported_property-' + country))
                    rp_subject.add_property(URI('rdf:type'), URI('meas:ReportedProperty'))
                    rp_subject.add_property(URI('rdf:type'), URI('meas:LengthAreaVolumeOrAngle'))
                    rv_subject.add_property(URI('meas:defined_by'), rp_subject)
                unit_subject = Subject(URI('isi:' + country + '-area-unit'))
                unit_subject.add_property(URI('rdf:type'), URI('cco:MeasuamentUnitOfArea'))
                unit_subject.add_property(URI('rdf:type'), URI('cco:SquareKilometerMeasurementUnit'))
                if measure_unit:
                    unit_subject.add_property(URI('rdfs:label'), Literal(measure_unit[0].value))

                rv_subject.add_property(URI('meas:has_measurement_unit'), unit_subject)

                # Add triples for population data
                rp_subject = Subject(URI('isi:population-reported_property-' + country))
                rp_subject.add_property(URI('rdf:type'), URI('meas:ReportedProperty'))
                rp_subject.add_property(URI('rdf:type'), URI('actor:PopulationOfConcern'))
                rp_subject.add_property(URI('meas:has_property_type'), URI('meas:Count'))
                if pop_value:
                    rv_subject = Subject(URI('isi:' + country + '-population'))
                    rv_subject.add_property(URI('rdf:type'), URI('meas:ReportedValue'))
                    rv_subject.add_property(URI('cco:has_decimal_value'),
                                            Literal(str(pop_value[0].value), type_=LiteralType.decimal))
                    rv_subject.add_property(URI('meas:defined_by'), rp_subject)
                    country_subject.add_property(URI('meas:has_reported_value'), rv_subject)

                # Add triples for age structure data
                if age_struct_values:
                    rp_subject = Subject(URI('isi:' + country + '-age_structure-reported_property'))
                    rp_subject.add_property(URI('rdf:type'), URI('meas:ReportedProperty'))
                    rp_subject.add_property(URI('rdf:type'), URI('meas:AgeDemographic'))
                    for i in range(len(age_struct_values)):
                        rv_subject = Subject(URI('isi:' + country + '-age_structure-' + age_struct_values[i]))
                        rv_subject.add_property(URI('rdf:type'), URI('meas:ReportedValue'))
                        country_subject.add_property(URI('meas:has_reported_value'), rv_subject)
                        rv_subject.add_property(URI('rdf:label'), Literal(age_struct_values[i]))
                        rv_subject.add_property(URI('cco:has_decimal_value'),
                                                Literal(str(age_struct_values_percent[i]), type_=LiteralType.decimal))
                        rv_subject.add_property(URI('meas:defined_by'), rp_subject)

                # Add triples for ethnicity data
                if ethnicity_values:
                    rv_subject = Subject(URI('isi:' + country + '-ethnic'))
                    rv_subject.add_property(URI('rdf:type'), URI('meas:ReportedValue'))
                    rp_subject.add_property(URI('rdf:type'), URI('meas:EthnicityDemographic'))
                    rp_subject.add_property(URI('meas:has_property_type'), URI('meas:RawValue'))
                    rp_subject = Subject(URI('isi:' + country + '-ethnic-reported_property'))
                    rp_subject.add_property(URI('rdf:type'), URI('meas:ReportedProperty'))
                    rv_subject.add_property(URI('meas:defined_by'), rp_subject)
                    country_subject.add_property(URI('meas:has_reported_value'), rv_subject)
                    for i in range(len(ethnicity_values)):
                        actor_subject.add_property(URI('actor:has_ethnicity'),
                                                   URI('eth:' + ethnicity_values[i].replace(':', '')))
                # Add triples for sex ratio data
                if sex_ratio_values:
                    rp_subject = Subject(URI('isi:' + country + '-sex_ratio-reported_property'))
                    rp_subject.add_property(URI('rdf:type'), URI('meas:ReportedProperty'))
                    for i in range(len(sex_ratio_values)):
                        rv_subject = Subject(URI('isi:' + country + '-sex_ratio-' + sex_ratio_key_values[i]))
                        rv_subject.add_property(URI('rdf:type'), URI('meas:ReportedValue'))
                        rv_subject.add_property(URI('rdfs:label'), Literal(sex_ratio_key_values[i]))
                        rv_subject.add_property(URI('cco:has_decimal_value'),
                                                Literal(str(sex_ratio_values[i]), type_=LiteralType.decimal))
                        rv_subject.add_property(URI('meas:defined_by'), rp_subject)
                        country_subject.add_property(URI('meas:has_reported_value'), rv_subject)

                # Add triples for religion data
                if religion_values:
                    rp_subject_religion = Subject(URI('isi:' + country + '-religion-reported_property'))
                    rp_subject_religion.add_property(URI('rdf:type'), URI('meas:ReportedProperty'))
                    rp_subject_religion.add_property(URI('rdf:type'), URI('eth:Religion'))
                    rp_subject_religion.add_property(URI('meas:has_property_type'), URI('meas:RawValue'))
                    for i in range(len(religion_values)):
                        rv_subject = Subject(
                            URI('isi:Religion-' + country + '-' + religion_values[i].replace(' ', '').replace('<', '')))
                        rv_subject.add_property(URI('rdf:type'), URI('meas:ReportedValue'))
                        rv_subject.add_property(URI('rdfs:label'), Literal(religion_values[i]))
                        rv_subject.add_property(URI('meas:defined_by'), rp_subject_religion)
                        rv_subject.add_property(URI('cco:has_decimal_value'),
                                                Literal(str(religion_percent_values[i]), type_=LiteralType.decimal))
                        actor_subject.add_property(URI('actor:has_affiliation'), rv_subject)

                # Add triples for mortality data
                if mor_dict:
                    rp_subject = Subject(URI('isi:' + country + '-mortality-reported_property'))
                    rp_subject.add_property(URI('rdf:type'), URI('meas:Mortality'))
                    rp_subject.add_property(URI('rdf:type'), URI('meas:ReportedProperty'))
                    for key in mor_dict:
                        rv_subject = Subject(URI('isi:' + country + '-' + key))
                        rv_subject.add_property(URI('rdf:type'), URI('meas:ReportedValue'))
                        rv_subject.add_property(URI('rdfs:label'), Literal(key))
                        rv_subject.add_property(URI('cco:has_decimal_value'),
                                                Literal(str(mor_dict[key]), type_=LiteralType.decimal))
                        rv_subject.add_property(URI('meas:defined_by'), rp_subject)
                        country_subject.add_property(URI('meas:has_reported_value'), rv_subject)

                # Add triples for GDP data
                if gdp_value:
                    rp_subject = Subject(URI('isi:' + country + '-gdp-reported_property'))
                    rp_subject.add_property(URI('rdf:type'), URI('meas:ReportedProperty'))
                    for i in gdp_value[0].value:
                        if 'data' in i:
                            rv_subject = Subject(URI('isi:' + country + '-gdp-' + str(i['date'])))
                            rv_subject.add_property(URI('rdf:type'), URI('meas:ReportedValue'))
                            rv_subject.add_property(URI('rdfs:label'), Literal(str(i['date'])))
                            rv_subject.add_property(URI('cco:has_decimal_value'),
                                                    Literal(str(i['value']), type_=LiteralType.decimal))
                            rv_subject.add_property(URI('meas:defined_by'), rp_subject)
                            unit_sub = Subject(URI('isi:' + country + '-gdp-unit'))
                            unit_sub.add_property(URI('rdf:type'), URI('cco:UnitedStatesDollar'))
                            unit_sub.add_property(URI('rdf:type'), URI('cco:CurrencyUnit'))
                            unit_sub.add_property(URI('rdfs:label'), Literal(i['units']))
                            rv_subject.add_property(URI('meas:has_measurement_unit'), unit_sub)
                            country_subject.add_property(URI('meas:has_reported_value'), rv_subject)

                # Add triples for labor force data
                if labor_force_value:
                    rp_subject = Subject(URI('isi:' + country + '-labor-reported_property'))
                    rp_subject.add_property(URI('rdf:type'), URI('meas:ReportedProperty'))
                    rp_subject.add_property(URI('meas:has_property_type'), URI('meas:Count'))
                    rp_subject.add_property(URI('rdf:type'), URI('meas:Labor'))
                    rv_subject = Subject(URI('isi:' + country + '-labor_force'))
                    rv_subject.add_property(URI('rdf:type'), URI('meas:ReportedValue'))
                    rv_subject.add_property(URI('cco:has_decimal_value'),
                                            Literal(str(labor_force_value[0].value), type_=LiteralType.decimal))
                    rv_subject.add_property(URI('meas:defined_by'), rp_subject)
                    country_subject.add_property(URI('meas:has_reported_value'), rv_subject)

                # Add triples for unemployment rate data
                if unem_rate_value:
                    rp_subject = Subject(URI('isi:' + country + '-unem-reported_property'))
                    rp_subject.add_property(URI('rdf:type'), URI('meas:ReportedProperty'))
                    rp_subject.add_property(URI('rdf:type'), URI('meas:UnemploymentRelated'))
                    for i in unem_rate_value[0].value:
                        if 'date' in i:
                            rv_subject = Subject(URI('isi:' + country + '-unemployment_rate-' + str(i['date'])))
                            rv_subject.add_property(URI('rdf:type'), URI('meas:ReportedValue'))
                            rv_subject.add_property(URI('rdfs:label'), Literal(str(i['date'])))
                            rv_subject.add_property(URI('cco:has_decimal_value'),
                                                    Literal(str(i['value']), type_=LiteralType.decimal))
                            rv_subject.add_property(URI('meas:defined_by'), rp_subject)
                            country_subject.add_property(URI('meas:has_reported_value'), rv_subject)

                # Add triples for military data
                if military_value:
                    rp_subject = Subject(URI('isi:' + country + '-military-reported_property'))
                    rp_subject.add_property(URI('rdf:type'), URI('meas:ReportedProperty'))
                    rp_subject.add_property(URI('rdf:type'), URI('meas:IncomeAndExpense'))
                    for i in military_value[0].value:
                        rv_subject = Subject(URI('isi:' + country + '-military_percent-' + str(i['date'])))
                        rv_subject.add_property(URI('rdf:type'), URI('meas:ReportedValue'))
                        rv_subject.add_property(URI('rdfs:label'), Literal(str(i['date'])))
                        rv_subject.add_property(URI('rdfs:label'), Literal('percent of GDP'))
                        rv_subject.add_property(URI('cco:has_decimal_value'),
                                                Literal(str(i['value']), type_=LiteralType.decimal))
                        rv_subject.add_property(URI('meas:defined_by'), rp_subject)
                        country_subject.add_property(URI('meas:has_reported_value'), rv_subject)

                # Finally we add all the triples in to kg
                doc.kg.add_subject(country_subject)
                # We only model one country in this example, break the loop
                break

        return list()


if __name__ == "__main__":
    kg_schema = KGSchema()
    onto_path = 'resources/preloaded-ontologies'
    ontologies = list()

    # Load ontologies in to kg_schema
    for dirpath, dirnames, filenames in os.walk(onto_path):
        for file in filenames:
            f = open(os.path.join(dirpath, file), 'r', encoding='utf-8')
            if os.path.splitext(file)[1] == '.ttl':
                ontologies.append(os.path.join(dirpath, file))
                onto_str = f.read()
                kg_schema.add_schema(onto_str, 'ttl')
            f.close()

    file_path = 'resources/factbook.json'
    f = open(file_path, 'r', encoding='utf-8')
    sample_input = json.loads(f.read())
    f.close()

    # Create ETK instance, load json dataset to create documents
    etk = ETK(kg_schema=kg_schema, modules=ExampleETKModule)
    doc = etk.create_document(sample_input, doc_id="http://isi.edu/default-ns/projects")

    docs = etk.process_ems(doc)

    # print(docs[0].kg.serialize('ttl'))
    # print(docs[0].kg.serialize('nt'))
    # print(docs[0].kg._resolve_uri.cache_info())

    # Generate triples and output as a file. Format can also be ttl or json-ld, etc.
    f = open('output_nt.nt', 'w')
    f.write(docs[0].kg.serialize('nt'))
    f.close()
