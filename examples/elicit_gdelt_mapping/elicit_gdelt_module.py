import os
import sys, json
from typing import List
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.etk import ETK
from etk.etk_module import ETKModule
from etk.csv_processor import CsvProcessor
from etk.extractors.date_extractor import DateExtractor
from etk.extractors.decoding_value_extractor import DecodingValueExtractor
from etk.document_selector import DefaultDocumentSelector
from etk.document import Document
from etk.knowledge_graph_schema import KGSchema
from etk.utilities import Utility
from examples.elicit_gdelt_mapping.elicit_gdelt_api import GdeltMapping


class GdeltModule(ETKModule):
    """
    ETK module to process gdelt event documents
    """

    header_fields = [
        "GLOBALEVENTID",
        "SQLDATE",
        "MonthYear",
        "Year",
        "FractionDate",
        "Actor1Code",
        "Actor1Name",
        "Actor1CountryCode",
        "Actor1KnownGroupCode",
        "Actor1EthnicCode",
        "Actor1Religion1Code",
        "Actor1Religion2Code",
        "Actor1Type1Code",
        "Actor1Type2Code",
        "Actor1Type3Code",
        "Actor2Code",
        "Actor2Name",
        "Actor2CountryCode",
        "Actor2KnownGroupCode",
        "Actor2EthnicCode",
        "Actor2Religion1Code",
        "Actor2Religion2Code",
        "Actor2Type1Code",
        "Actor2Type2Code",
        "Actor2Type3Code",
        "IsRootEvent",
        "EventCode",
        "EventBaseCode",
        "EventRootCode",
        "QuadClass",
        "GoldsteinScale",
        "NumMentions",
        "NumSources",
        "NumArticles",
        "AvgTone",
        "Actor1Geo_Type",
        "Actor1Geo_FullName",
        "Actor1Geo_CountryCode",
        "Actor1Geo_ADM1Code",
        "Actor1Geo_Lat",
        "Actor1Geo_Long",
        "Actor1Geo_FeatureID",
        "Actor2Geo_Type",
        "Actor2Geo_FullName",
        "Actor2Geo_CountryCode",
        "Actor2Geo_ADM1Code",
        "Actor2Geo_Lat",
        "Actor2Geo_Long",
        "Actor2Geo_FeatureID",
        "ActionGeo_Type",
        "ActionGeo_FullName",
        "ActionGeo_CountryCode",
        "ActionGeo_ADM1Code",
        "ActionGeo_Lat",
        "ActionGeo_Long",
        "ActionGeo_FeatureID",
        "DATEADDED",
        "SOURCEURL"
    ]

    ontology_prefixes = {
        "event": "http://ontology.causeex.com/ontology/odps/EventHierarchy#"
    }

    # Gets populated in the init method
    header_translation_table = {}

    # The property to use when ontologizing the relationship of an event to an actor.
    actor_role = {
        "event:has_active_actor": "actor",
        "event:has_actor": "participant",
        "event:has_recipient": "recipient",
        "event:has_provider": "provider",
        "event:has_affected_actor": "victim"
    }

    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.mapping = GdeltMapping(json.load(open("ODP-Mappings-V3.1.json")))
        # As our input files have no header, create a translation table to go from names to indices.
        for i in range(0, len(self.header_fields)):
            self.header_translation_table[self.header_fields[i]] = "COL" + str(i)
        # Extractors
        self.date_extractor = DateExtractor(self.etk, "Date Extractor")

    def attribute(self, attribute_name: str):
        """
        Translate an attribute_name to the key in the JSON
        Args:
            attribute_name: attribute names as given in the code book

        Returns: the key in the JSON document

        """
        return self.header_translation_table[attribute_name]

    def attribute_value(self, doc: Document, attribute_name: str):
        """
        Access data using attribute name rather than the numeric indices

        Returns: the value for the attribute

        """
        return doc.cdr_document.get(self.header_translation_table[attribute_name])

    def expand_prefix(self, uri: str):
        prefix, name = uri.split(":")
        if prefix and name:
            return self.ontology_prefixes.get(prefix) + name
        else:
            return None

    def add_actors(self, doc:Document, event: str, cameo_code: int) -> List[Document]:
        """
        Each event has two actors. The relationship of the event to the actors depends
        on the cameo code and is defined by the mapping.
        Args:
            doc: the document containing the evence
            event: one of "event1", "event2", or "event3"
            cameo_code:

        Returns: the documents created for each actor

        """
        # Actor1
        actor1_cdr = {
            "ActorName": doc.cdr_document[self.attribute("Actor1Name")],
            "ActorCountryCode": doc.cdr_document[self.attribute("Actor1CountryCode")],
            "ActorKnownGroupCode": doc.cdr_document[self.attribute("Actor1KnownGroupCode")],
            "ActorEthnicCode": doc.cdr_document[self.attribute("Actor1EthnicCode")],
            "ActorReligion1Code": doc.cdr_document[self.attribute("Actor1Religion1Code")],
            "ActorReligion2Code": doc.cdr_document[self.attribute("Actor1Religion2Code")],
            "ActorType1Code": doc.cdr_document[self.attribute("Actor1Type1Code")],
            "ActorType2Code": doc.cdr_document[self.attribute("Actor1Type2Code")],
            "ActorType3Code": doc.cdr_document[self.attribute("Actor1Type3Code")],
            "ActorGeo_Type": doc.cdr_document[self.attribute("Actor1Geo_Type")],
            "ActorGeo_FullName": doc.cdr_document[self.attribute("Actor1Geo_FullName")],
            "ActorGeo_CountryCode": doc.cdr_document[self.attribute("Actor1Geo_CountryCode")],
            "ActorGeo_ADM1Code": doc.cdr_document[self.attribute("Actor1Geo_ADM1Code")],
            "ActorGeo_Lat": doc.cdr_document[self.attribute("Actor1Geo_Lat")],
            "ActorGeo_Long": doc.cdr_document[self.attribute("Actor1Geo_Long")],
            "ActorGeo_FeatureID": doc.cdr_document[self.attribute("Actor1Geo_FeatureID")],
            "dataset": "gdelt-actor"
        }
        actor1 = etk.create_document(actor1_cdr)
        actor1.doc_id = doc.doc_id + "-actor1"

        # Link actor1 to the event
        actor_field = "participant"
        actor_prop = self.mapping.actor_property(event, "actor1", cameo_code)
        if actor_prop and self.actor_role.get(actor_prop):
            actor_field = self.actor_role.get(actor_prop)
        doc.kg.add_value(actor_field, actor1.doc_id)

        # Actor2
        actor2_cdr = {
            "ActorName": doc.cdr_document[self.attribute("Actor2Name")],
            "ActorCountryCode": doc.cdr_document[self.attribute("Actor2CountryCode")],
            "ActorKnownGroupCode": doc.cdr_document[self.attribute("Actor2KnownGroupCode")],
            "ActorEthnicCode": doc.cdr_document[self.attribute("Actor2EthnicCode")],
            "ActorReligion1Code": doc.cdr_document[self.attribute("Actor2Religion1Code")],
            "ActorReligion2Code": doc.cdr_document[self.attribute("Actor2Religion2Code")],
            "ActorType1Code": doc.cdr_document[self.attribute("Actor2Type1Code")],
            "ActorType2Code": doc.cdr_document[self.attribute("Actor2Type2Code")],
            "ActorType3Code": doc.cdr_document[self.attribute("Actor2Type3Code")],
            "ActorGeo_Type": doc.cdr_document[self.attribute("Actor2Geo_Type")],
            "ActorGeo_FullName": doc.cdr_document[self.attribute("Actor2Geo_FullName")],
            "ActorGeo_CountryCode": doc.cdr_document[self.attribute("Actor2Geo_CountryCode")],
            "ActorGeo_ADM1Code": doc.cdr_document[self.attribute("Actor2Geo_ADM1Code")],
            "ActorGeo_Lat": doc.cdr_document[self.attribute("Actor2Geo_Lat")],
            "ActorGeo_Long": doc.cdr_document[self.attribute("Actor2Geo_Long")],
            "ActorGeo_FeatureID": doc.cdr_document[self.attribute("Actor2Geo_FeatureID")],
            "dataset": "gdelt-actor"
        }
        actor2 = etk.create_document(actor2_cdr)
        actor2.doc_id = doc.doc_id + "-actor2"

        # Link actor2 to the event
        actor_field = "participant"
        actor_prop = self.mapping.actor_property(event, "actor2", cameo_code)
        if actor_prop and self.actor_role.get(actor_prop):
            actor_field = self.actor_role.get(actor_prop)
        doc.kg.add_value(actor_field, actor2.doc_id)

        return [actor1, actor2]

    def document_selector(self, doc: Document) -> bool:
        return doc.cdr_document.get("dataset") == "gdelt"

    def process_document(self, doc: Document) -> List[Document]:
        # Record new documents we create that need to be processed later.
        new_docs = list()

        cameo_code = self.attribute_value(doc, "EventCode")
        print("Processing cameo code {}".format(cameo_code))

        doc.doc_id = str(doc.cdr_document[self.attribute("GLOBALEVENTID")])

        # Type
        doc.kg.add_value("type", "Event")
        if self.mapping.has_cameo_code(cameo_code):
            # Type fields
            for t in self.mapping.event_type("event1", cameo_code):
                doc.kg.add_value("type", value=t)
                doc.kg.add_value("causeex_class", value=self.expand_prefix(t))

        # Event_date
        for s in doc.select_segments("$." + self.attribute("SQLDATE")):
            doc.kg.add_value("event_date", value=doc.extract(self.date_extractor, s,
                                                       prefer_language_date_order=None,
                                                       additional_formats=["%Y%m%d"],
                                                       detect_relative_dates=False,
                                                       use_default_formats=False))

        # CAMEO code
        cameo_code_label = "CAMEO Code: " + str(doc.select_segments("$." + self.attribute("EventCode"))[0].value)
        doc.kg.add_value("code", value=cameo_code_label)
        # simpler without provenance:
        # doc.kg.add_value("code", "CAMEO Code: " + doc.cdr_document[self.attribute("EventCode")])

        # Identifier
        doc.kg.add_value("identifier", json_path="$." + self.attribute("GLOBALEVENTID"))

        # Geographical information
        doc.kg.add_value("country_code", json_path="$." + self.attribute("ActionGeo_CountryCode"))
        doc.kg.add_value("location", json_path="$." + self.attribute("ActionGeo_FullName"))

        # Actors
        actor1, actor2 = self.add_actors(doc, "event1", cameo_code)
        new_docs.append(actor1)
        new_docs.append(actor2)

        # has topic events
        for event in ["event2", "event3"]:
            new_docs.extend(self.add_topic_events(doc, event, actor1.doc_id, actor2.doc_id))

        return new_docs

    def add_topic_events(self, doc: Document, event: str, actor1_docid: str, actor2_docid: str) -> List[Document]:
        new_docs = list()
        cameo_code = self.attribute_value(doc, "EventCode")
        topic_event_type = self.mapping.has_event(event, cameo_code)
        if topic_event_type:
            topic_cdr = {
                "causex_type": topic_event_type,
                "dataset": "gdelt-topic-event"
            }
            topic_event = etk.create_document(topic_cdr)
            new_docs.append(topic_event)
            topic_event.doc_id = doc.doc_id + "-" + event

            # Link the main event and the topic_event
            # In the ODP file, the pattern is always (has_topic of event1), so we use has_topic all the time
            topic_event.kg.add_value("event_topic", value=doc.doc_id)

            # Title
            topic_event.kg.add_value("title", value=topic_event_type)

            # Type
            topic_event.kg.add_value("type", value=["Event", topic_event_type])

            # causeex_class
            topic_event.kg.add_value("causeex_class", value=topic_event_type)

            # Actors:
            # Use the same actor objects as in the main event and use the mapping file to assign the roles
            for docid, role in zip([actor1_docid, actor2_docid], ["actor1", "actor2"]):
                actor_field = "participant"
                actor_prop = self.mapping.actor_property(event, role, cameo_code)
                if actor_prop and self.actor_role.get(actor_prop):
                    actor_field = self.actor_role.get(actor_prop)
                topic_event.kg.add_value(actor_field, value=docid)

            # Unclear whether the location, etc. of the topic event should be the same as in the main event.
            topic_event.kg.add_value("country_code", value=doc.kg.get_values("country_code"))
            topic_event.kg.add_value("location", value=doc.kg.get_values("location"))
            topic_event.kg.add_value("event_date", value=doc.kg.get_values("event_date"))
        return new_docs


class GdeltActorModule(ETKModule):
    """
    ETK module to map actor objects
    """

    # from GDLET docs: https://www.gdeltproject.org/data/lookups/CAMEO.type.txt
    actor_codes = {
        "COP": "Police forces",
        "GOV": "Government",
        "INS": "Insurgents",
        "JUD": "Judiciary",
        "MIL": "Military",
        "OPP": "Political Opposition",
        "REB": "Rebels",
        "SEP": "Separatist Rebels",
        "SPY": "State Intelligence",
        "UAF": "Unaligned Armed Forces",
        "AGR": "Agriculture",
        "BUS": "Business",
        "CRM": "Criminal",
        "CVL": "Civilian",
        "DEV": "Development",
        "EDU": "Education",
        "ELI": "Elites",
        "ENV": "Environmental",
        "HLH": "Health",
        "HRI": "Human Rights",
        "LAB": "Labor",
        "LEG": "Legislature",
        "MED": "Media",
        "REF": "Refugees",
        "MOD": "Moderate",
        "RAD": "Radical",
        "AMN": "Amnesty International",
        "IRC": "Red Cross",
        "GRP": "Greenpeace",
        "UNO": "United Nations",
        "PKO": "Peacekeepers",
        "UIS": "Unidentified State Actor",
        "IGO": "Inter-Governmental Organization",
        "IMG": "International Militarized Group",
        "INT": "International/Transnational Generic",
        "MNC": "Multinational Corporation",
        "NGM": "Non-Governmental Movement",
        "NGO": "Non-Governmental Organization",
        "SET": "Settler"
    }

    # from https://www.gdeltproject.org/data/lookups/CAMEO.knowngroup.txt
    known_group_codes = {
        "AAM": "Al Aqsa Martyrs Brigade",
        "ABD": "Arab Bank for Economic Development in Africa",
        "ACC": "Arab Cooperation Council",
        "ADB": "Asian Development Bank",
        "AEU": "Arab Economic Unity Council",
        "AFB": "African Development Bank",
        "ALQ": "Al Qaeda",
        "AMF": "Arab Monetary Fund for Economic and Social Development",
        "AML": "Amal Militia",
        "AMN": "Amnesty International",
        "AMU": "Arab Maghreb Union",
        "ANO": "Abu Nidal Organization",
        "APE": "Org. of Arab Petroleum Exporting Countries (OAPEC)",
        "ARL": "Arab League",
        "ASL": "South Lebanon Army",
        "ASN": "Association of Southeast Asian Nations (ASEAN)",
        "ATD": "Eastern and Southern African Trade and Development Bank",
        "BCA": "Bank of Central African States (BEAC)",
        "BIS": "Bank for International Settlements",
        "BTH": "Baath Party",
        "CEM": "Monetary and Economic Community of Central Africa",
        "CFA": "Franc Zone Financial Community of Africa",
        "CIS": "Commonwealth of Independent States",
        "CMN": "Communist",
        "COE": "Council of Europe",
        "CPA": "Cocoa Producer's Alliance",
        "CPC": "Association of Coffee Producing Countries",
        "CRC": "International Fed. of Red Cross and Red Crescent (ICRC)",
        "CSS": "Community of Sahel-Saharan States (CENSAD)",
        "CWN": "Commonwealth of Nations",
        "DFL": "Democratic Front for the Lib. of Palestine (DFLP)",
        "EBR": "European Bank for Reconstruction and Development",
        "ECA": "Economic Community of Central African States",
        "EEC": "European Union",
        "EFT": "European Free Trade Association",
        "ENN": "Ennahda Movement",
        "FAO": "United Nations Food and Agriculture Organization",
        "FID": "International Federation of Human Rights (FIDH)",
        "FIS": "Islamic Salvation Army",
        "FLN": "National Liberation Front (FLN)",
        "FTA": "Fatah",
        "GCC": "Gulf Cooperation Council",
        "GIA": "Armed Islamic Group (GIA)",
        "GOE": "Group of Eight (G-8) (G-7 plus Russia)",
        "GOS": "Group of Seven (G-7)",
        "GSP": "Salafist Group",
        "GSS": "Group of Seventy-Seven (G-77)",
        "HCH": "UN High Commission for Human Rights",
        "HCR": "UN High Commission for Refugees",
        "HEZ": "Hezbullah",
        "HIP": "Highly Indebted Poor Countries (HIPC)",
        "HMS": "Hamas",
        "HRW": "Human Rights Watch",
        "IAC": "Inter-African Coffee Organization (IACO)",
        "IAD": "Intergovernmental Authority on Development (IGAD)",
        "IAE": "International Atomic Energy Agency (IAEA)",
        "IAF": "Islamic Action Front",
        "ICC": "International Criminal Court",
        "ICG": "International Crisis Group",
        "ICJ": "International Court of Justice (ICJ)",
        "ICO": "International Cocoa Organization (ICCO)",
        "IDB": "Islamic Development Bank",
        "IGC": "International Grains Council",
        "IHF": "International Helsinki Federation for Human Rights",
        "ILO": "International Labor Organization",
        "IMF": "International Monetary Fund (IMF)",
        "IOM": "International Organization for Migration",
        "IPU": "Inter-Parliamentary Union",
        "IRC": "Red Cross",
        "ISJ": "Palestinian Islamic Jihad",
        "ITP": "Interpol",
        "JUR": "International Commission of Jurists",
        "KDP": "Kurdish Democratic Party (KDP)",
        "KID": "United Nations Children?s Fund (UNICEF)",
        "LBA": "Israeli Labor Party",
        "LKD": "Likud Party",
        "MBR": "Muslim Brotherhood",
        "MRZ": "Meretz Party",
        "MSF": "Medecins Sans Frontieres (Doctors Without Borders)",
        "MSP": "Movement of the Society for Peace",
        "NAT": "North Atlantic Treaty Organization (NATO)",
        "NEP": "New Economic Partnership for Africa?s Development",
        "NON": "Organization of Non-Aligned Countries",
        "OAS": "Organization of American States",
        "OAU": "Organization of African Unity (OAU)",
        "OIC": "Organization of Islamic Conferences (OIC)",
        "OPC": "Organization of Petroleum Exporting Countries (OPEC)",
        "PAP": "Pan-African Parliament",
        "PFL": "People's Front for the Liberation of Palestine (PFLP)",
        "PLF": "Palestine Liberation Front",
        "PLO": "Palestine Liberation Organization",
        "PLS": "Polisario Guerillas",
        "PMD": "People's Mujahedeen",
        "PRC": "Paris Club",
        "PSE": "Occupied Palestinian Territories",
        "RCR": "Red Crescent",
        "RND": "Democratic National Rally",
        "SAA": "South Asian Association",
        "SAD": "Southern African Development Community",
        "SCE": "Council of Security and Cooperation in Europe (OSCE)",
        "SHA": "Shas Party",
        "SOT": "Southeast Asia Collective Defense Treaty (SEATO)",
        "TAL": "Taliban",
        "UEM": "Economic and Monetary Union of West Africa (UEMOA)",
        "UNO": "United Nations",
        "WAD": "West Africa Development Bank",
        "WAM": "West Africa Monetary and Economic Union",
        "WAS": "Economic Community of West African States (ECOWAS)",
        "WBK": "World Bank",
        "WCT": "International War Crimes Tribunals",
        "WEF": "World Economic Forum",
        "WFP": "World Food Program",
        "WHO": "World Health Organization",
        "WTO": "World Trade Organization (WTO)",
        "XFM": "Oxfam"
    }

    # form: https://www.gdeltproject.org/data/lookups/CAMEO.ethnic.txt
    ethnic_codes = {
        "aar": "Afar",
        "abk": "Abkhaz",
        "abr": "Aboriginal-Australians",
        "ace": "Acehnese",
        "acg": "Achang",
        "ach": "Acholi",
        "ada": "Ga",
        "adi": "Adivasi",
        "adj": "Adjarians",
        "ady": "Adyghe",
        "afa": "Black-African",
        "afr": "Afrikaners",
        "ahm": "Ahmadis",
        "ain": "Ainu",
        "aja": "Aja",
        "aka": "Akan",
        "aku": "Aku",
        "ala": "Alawi",
        "alb": "Albanian",
        "ale": "Aleut",
        "alg": "Algonquian",
        "alt": "Altay",
        "alu": "Alur",
        "amb": "Ambonese",
        "ame": "Americo-Liberians",
        "amh": "Amhara",
        "anp": "Angika speakers",
        "apa": "Apache",
        "ara": "Arab",
        "ARB": "Arab",
        "arg": "Aragonese",
        "arm": "Armenian",
        "arn": "Mapuche",
        "arp": "Arapaho",
        "arw": "Arawak",
        "asa": "Asian",
        "ash": "Ashkenazi Jews",
        "asm": "Assamese",
        "ast": "Asturian",
        "asy": "Assyrian",
        "ata": "Atacamenos",
        "atg": "Argentinians",
        "ath": "Athabaskan",
        "ats": "Agnostic/Athiest",
        "aus": "Australians",
        "auu": "Austrians",
        "ava": "Caucasian Avars",
        "awa": "Awadhi",
        "aym": "Aymara",
        "aze": "Azerbaijani",
        "bad": "Baganda",
        "bah": "Bahais",
        "bai": "Bamileke",
        "bak": "Bashkirs",
        "bal": "Baloch",
        "bam": "Bambara",
        "ban": "Balinese",
        "baq": "Basque",
        "bar": "Bari",
        "bas": "Basoga",
        "bay": "Gbaya",
        "bda": "Rakhine",
        "bej": "Beja",
        "bel": "Belarusians",
        "bem": "Bemba",
        "ben": "Bengali-Hindu",
        "ber": "Berber",
        "bey": "Beydan",
        "bho": "Bhojpuri",
        "bih": "Bihari",
        "bii": "Bai",
        "bik": "Bicolano",
        "bin": "Edo",
        "bis": "Urban ni-Vanautu",
        "bke": "Bateke",
        "bkn": "Bakongo",
        "bkw": "Bakweri",
        "bla": "Siksikawa",
        "blg": "Blang",
        "blk": "Balkars",
        "bln": "Balanta",
        "bmr": "Bamar",
        "bni": "Beni-Shugal-Gumez",
        "bnt": "Bantu",
        "bny": "Banyarwanda",
        "bod": "Tibetan",
        "bol": "Bolivia",
        "bon": "Bonan",
        "bos": "Bosniaks",
        "bou": "Buyei",
        "bra": "Brijwasi",
        "brb": "Bariba",
        "bre": "Breton",
        "brh": "Brahui",
        "brk": "Burakumin",
        "brm": "Kurichiya",
        "bsh": "Bushmen",
        "bst": "Baster",
        "bsu": "Subiya",
        "bte": "Beti-Pahuin",
        "btk": "Batak",
        "bua": "Buryat",
        "bud": "Buddhist",
        "bug": "Bugis",
        "bul": "Bulgarian",
        "byn": "Bilen",
        "cab": "Cabindan-Mayombe",
        "cad": "Caddo",
        "cap": "Cape Verdean",
        "car": "Kali'na",
        "cat": "Catalan",
        "ceb": "Cebuano",
        "cha": "Chamorro",
        "chc": "Chukchi",
        "che": "Chechen",
        "chg": "Chagatai",
        "chi": "Chinese",
        "chk": "Chuukese",
        "chl": "Chileans",
        "chm": "Mari",
        "chn": "Chinook",
        "cho": "Choctaw",
        "chp": "Chipewyan",
        "chr": "Cherokee",
        "cht": "Ch'orti'",
        "chv": "Chuvash",
        "chw": "Chewa",
        "chy": "Cheyenne",
        "cir": "Adyghe",
        "cmc": "Cham",
        "col": "Colombian",
        "con": "Confusian",
        "cop": "Coptic Christians",
        "cor": "Cornish",
        "cos": "Corsican",
        "cot": "Cotiers",
        "cpe": "English-Creole",
        "cpf": "French-Creole",
        "cpp": "Portuguese-Creole",
        "cre": "Cree",
        "crh": "Crimean Tatar",
        "cri": "Christian",
        "cro": "Orthodox Christian",
        "crp": "Creole",
        "csb": "Kashubian",
        "csr": "Costa Ricans",
        "cth": "Catholics",
        "cus": "Cushitic",
        "cze": "Czech",
        "dai": "Dai",
        "dak": "Sioux",
        "dal": "Dalit",
        "dam": "Damara",
        "dan": "Danes",
        "dao": "Yao (Asia)",
        "dar": "Dargwa",
        "dau": "Daur",
        "day": "Dayak",
        "del": "Lenape",
        "den": "Slavey",
        "dgr": "Dogrib",
        "din": "Dinka",
        "div": "Maldivian",
        "dje": "Djerma-Songhai",
        "doi": "Dogras",
        "dom": "Dominicans",
        "don": "Dong",
        "dox": "Dongxiang",
        "dra": "Dravidian",
        "dru": "Druze",
        "drz": "Druze",
        "dsb": "Lower Sorbian",
        "dua": "Duala",
        "dut": "Dutch",
        "dyu": "Dyula",
        "dzo": "Ngalop",
        "eat": "East Timorese",
        "ecu": "Ecuadorians",
        "efi": "Efik",
        "ein": "East Indian",
        "eka": "Ekajuk",
        "eng": "English",
        "esh": "Eshira",
        "est": "Estonian",
        "eth": "Ethiopian-Jews",
        "eur": "Europeans",
        "eve": "Evenks",
        "ewe": "Ewe",
        "ewo": "Ewondo",
        "fan": "Fang",
        "fao": "Faroese",
        "fat": "Fante",
        "fij": "Fijian",
        "fil": "Filipino",
        "fin": "Finns",
        "fiu": "Finno-Ugric",
        "fon": "Fon",
        "fre": "French",
        "fri": "Santals",
        "frr": "Frisians",
        "fru": "Fur",
        "ful": "Fula",
        "fur": "Friulan",
        "gar": "Garifuna",
        "gay": "Gayo",
        "gba": "Gbaya",
        "gel": "Gelao",
        "geo": "Georgian",
        "ger": "German",
        "gia": "Gia Rai",
        "gil": "Kiribati",
        "gin": "Gin",
        "gio": "Gio",
        "gla": "Gaels",
        "gle": "Irish",
        "glg": "Galician",
        "glv": "Manx",
        "gon": "Gondi",
        "gor": "Gorontalonese",
        "gra": "Grassfielders",
        "grb": "Grebo",
        "gre": "Greek",
        "grn": "Guarani",
        "gsw": "Swiss Germans",
        "gua": "Guatemalan",
        "guj": "Gujarati",
        "gun": "Guan",
        "gwi": "Gwich'in",
        "had": "Hadjerai",
        "hai": "Haida",
        "har": "Harari",
        "hat": "Haitian",
        "hau": "Hausa",
        "haw": "Hawaiian",
        "haz": "Hazara",
        "her": "Herero",
        "hgh": "Hill Tribes",
        "hil": "Hiligayon",
        "him": "Himachali",
        "hin": "Hindu",
        "hjw": "Hasidic",
        "hmn": "Hmong",
        "hmo": "Hiri Motu",
        "hni": "Hani",
        "hoa": "Hoa",
        "hon": "Hondurans",
        "hrt": "Haratin",
        "hrv": "Croats",
        "hsb": "Upper Sorbian",
        "hui": "Hui",
        "hun": "Hungarian",
        "hup": "Hupa",
        "hut": "Hutu",
        "iba": "Iban",
        "ibo": "Igbo",
        "ice": "Icelanders",
        "idg": "Indigenous",
        "idn": "Indian",
        "iii": "Yi",
        "ijo": "Ijaw",
        "iku": "Inuit",
        "ilo": "Ilocono",
        "ind": "Indonesian",
        "inh": "Ingush",
        "ipk": "Inupiat",
        "ira": "Iranian",
        "iro": "Iroquois",
        "ita": "Itallian",
        "jan": "Jain",
        "jav": "Javanese",
        "jew": "Jewish",
        "jhw": "Jehovah's Witnesses",
        "jin": "Jino",
        "jol": "Jola",
        "jpn": "Japanese",
        "kaa": "Karakalpak",
        "kab": "Kabyle",
        "kac": "Kachin",
        "kad": "Kadazan",
        "kak": "Kakwa-Nubian",
        "kal": "Kalaallit",
        "kam": "Kamba",
        "kan": "Kannada",
        "kao": "Kaonde",
        "kar": "Karen",
        "kas": "Kashmiri",
        "kau": "Kanuri",
        "kav": "Kavango",
        "kaz": "Kazakhs",
        "kbd": "Kabarday",
        "kby": "Kabye",
        "kch": "Karachays",
        "kha": "Khasi",
        "khi": "Khoikhoi",
        "khk": "Khakas",
        "khm": "Khmer",
        "khu": "Khmu",
        "kik": "Kikuyu",
        "kin": "Kinyarwanda Speakers",
        "kir": "Kyrgyz",
        "kis": "Kisii",
        "klm": "Kalmyk",
        "kmb": "North Mbundu",
        "kno": "Kono",
        "knr": "Kanuri",
        "kok": "Kokani",
        "kom": "Komi",
        "kon": "Kongo",
        "kor": "Korean",
        "kos": "Kosraean",
        "kou": "Kouyou",
        "kpe": "Kpelle",
        "krh": "Krahn",
        "krl": "Karelians",
        "krm": "Karamojong",
        "kro": "Kru",
        "kru": "Kurukh",
        "kua": "Kwanyama",
        "kum": "Kumyks",
        "kur": "Kurd",
        "KUR": "Kurd",
        "kut": "Ktunaxa",
        "lad": "Sephardic Jew",
        "lak": "Lak (Russia)",
        "lam": "Lamba",
        "lao": "Lao",
        "lar": "Lari",
        "lav": "Latvian",
        "lba": "Limba",
        "lds": "Latter Day Saints",
        "len": "Lenca",
        "lez": "Lezgian",
        "lgb": "Lugbara",
        "lhu": "Lahu",
        "lii": "Li",
        "lim": "Limburgian",
        "lin": "Lingala",
        "lit": "Lithuanian",
        "lol": "Mongo",
        "lom": "Lomwe",
        "lov": "Lovale",
        "loz": "Lozi",
        "lsu": "Lisu",
        "ltk": "Latoka",
        "ltn": "Latinos",
        "ltz": "Luxembourgers",
        "lua": "Luba-Kasai",
        "lub": "Luba-Katanga",
        "lug": "Baganda",
        "luh": "Luhya",
        "lui": "Luiseno",
        "lul": "Lulua",
        "lun": "Lunda",
        "luo": "Luo",
        "lus": "Lusei",
        "mac": "Macedonian",
        "mad": "Madurese",
        "maf": "Mafwe",
        "mag": "Magahi",
        "mah": "Marshallese",
        "mai": "Maithili",
        "mak": "Makassarese",
        "mal": "Malayalam",
        "man": "Mandinka",
        "mao": "Maori",
        "mar": "Marathi",
        "mas": "Maasai",
        "may": "Malays",
        "mba": "Mbandja",
        "mbe": "Mbere",
        "mbk": "M'Baka",
        "mbo": "Mbochi",
        "mbu": "Mbundu-Mestico",
        "mdf": "Mokshas",
        "mdh": "Madhesi",
        "mdi": "Madi",
        "mdr": "Mandar",
        "men": "Mende",
        "mia": "Miao",
        "mic": "Mi'kmaq",
        "mij": "Mijikenda",
        "min": "Minangkabau",
        "miz": "Mizo",
        "mla": "Mulatto",
        "mld": "Mole-Dagbani",
        "mlg": "Malagasy",
        "mlo": "Mulao",
        "mlt": "Maltese",
        "mnc": "Manchu",
        "mnd": "Mande",
        "mng": "Mananja-Nayanja",
        "mnh": "Minahasa",
        "mni": "Manipuri",
        "mnj": "Manjack",
        "mnn": "Mano",
        "mno": "Lumad",
        "mns": "Mon",
        "mny": "Manyika",
        "moh": "Mohawk",
        "mok": "Makonde",
        "mon": "Mongol",
        "mos": "Mossi",
        "mri": "Mari",
        "mrn": "Maronites",
        "mro": "Moro",
        "msk": "Miskito",
        "msl": "Muslim",
        "mtn": "Montenegrins",
        "mtz": "Mestizo",
        "mun": "Munda",
        "muo": "Muong",
        "mus": "Muscogee",
        "mwl": "Mirandese",
        "mwr": "Marwaris",
        "mya": "Mayangnas",
        "mye": "Myene",
        "myn": "Maya",
        "myv": "Mordvins",
        "nag": "Naga",
        "nah": "Nahua",
        "nai": "Native American",
        "nam": "Nama",
        "nap": "Neapolitan",
        "nau": "Nauruan",
        "nav": "Navajo",
        "nax": "Nakhi",
        "nba": "Nuba",
        "nbl": "South Ndebele",
        "nca": "Nicaraguan",
        "nde": "Northern Ndebele",
        "ndo": "Ndonga",
        "nep": "Nepali",
        "ner": "Nuer",
        "new": "Newars",
        "ngn": "Ngbandi",
        "ngo": "Ngoni",
        "nia": "Niasans",
        "nib": "Nibolek",
        "nir": "Niari",
        "niu": "Niuean",
        "nkm": "Nkomi",
        "nng": "Nung",
        "nog": "Nogais",
        "nor": "Norwegians",
        "nso": "Northern Sotho",
        "nub": "Nubian",
        "nur": "Nuristani",
        "nuu": "Nu",
        "nya": "Chewa",
        "nyk": "Nyakyusa",
        "nym": "Nyamwezi",
        "nyn": "Ankole",
        "nyo": "Nyoro",
        "nze": "New Zealanders",
        "nzi": "Nzema",
        "oci": "Occitanians",
        "ogo": "Ogoni",
        "oji": "Ojibwe",
        "ojw": "Orthodox/Ultra-Orthodox Jew",
        "oki": "Okinawan",
        "ori": "Oriya",
        "orm": "Oromo",
        "oru": "Orgunu",
        "osa": "Osage",
        "oss": "Ossetians",
        "oto": "Otomi",
        "ova": "Ovambo",
        "paa": "Papuan",
        "pac": "Pacific Islanders",
        "pag": "Pangasinan",
        "pal": "Palestinian",
        "PAL": "Palestinian",
        "pam": "Kapampangan",
        "pan": "Punjabi",
        "pap": "Papiamento-Creole",
        "par": "Paraguayan",
        "pau": "Palauan",
        "per": "Persian",
        "pgn": "Animist/Pagan",
        "phu": "Puthai",
        "pnm": "Panamanians",
        "pol": "Poles",
        "pom": "Pomaks",
        "pon": "Pehnpeian",
        "por": "Portuguese ",
        "ppl": "Papel",
        "pro": "Protestant",
        "pru": "Peruvian",
        "psh": "Pashayi",
        "pum": "Pumi",
        "pus": "Pashtun",
        "qia": "Qiang",
        "qiz": "Qizilbash",
        "que": "Quechua",
        "raj": "Rajasthani",
        "ran": "Pahari Rajput",
        "rap": "Rapa Nui",
        "rar": "Cook Islands Maori",
        "rel": "Unspecified Religion",
        "roh": "Romansh",
        "rom": "Romani",
        "rum": "Romanian",
        "run": "Rundi",
        "rup": "Aromanians",
        "rus": "Russian",
        "sad": "Sandawe",
        "sag": "Sango",
        "sah": "Yakuts",
        "sal": "Salish",
        "sar": "Sara",
        "sas": "Sasak",
        "sat": "Sudanese",
        "scn": "Sicilian",
        "sco": "Scottish",
        "sel": "Selkup",
        "sen": "Sena",
        "sfi": "Sufi",
        "sha": "Shafi'i",
        "she": "She",
        "shi": "Shi'ites",
        "shl": "Shilluk",
        "shn": "Shan",
        "shy": "Shaigiya",
        "sid": "Sidama",
        "sin": "Sinhalese",
        "sio": "Siouan",
        "sla": "Slavic",
        "slo": "Slovaks",
        "slr": "Salar",
        "slv": "Slovenes",
        "smi": "Sami",
        "smo": "Samoans",
        "sna": "Shona",
        "snd": "Sindhi",
        "snk": "Soninke",
        "som": "Somali",
        "son": "Songhai",
        "sot": "Sotho",
        "spa": "Spanish",
        "srd": "Sardinian",
        "srn": "Sranan Tongo",
        "srp": "Serbs",
        "srr": "Serer",
        "ssw": "Swazi",
        "sui": "Sui",
        "suk": "Sukama",
        "sun": "Sunni",
        "sus": "Susu",
        "swa": "Swahili",
        "swe": "Swedes",
        "swf": "Swiss French",
        "swt": "Swiss Italian",
        "tab": "Tabasaran",
        "tah": "Tahitian",
        "tai": "Tai",
        "tam": "Tamil",
        "tao": "Taoist",
        "tat": "Tatars",
        "taw": "Tawahka",
        "tay": "Tay",
        "tel": "Telugu",
        "tem": "Temne",
        "ter": "Terenan",
        "tes": "Teso",
        "tet": "Tetum",
        "tgk": "Tajik",
        "tgl": "Tagalog",
        "tha": "Thai",
        "tib": "Tibetan",
        "tig": "Tigre",
        "tir": "Tigray-Tigrinya",
        "tiv": "Tiv",
        "tkl": "Tokelauan",
        "tli": "Tlingit",
        "tmh": "Tuareg",
        "tms": "Tama",
        "tog": "Tonga (Africa)",
        "ton": "Tonga (Pacific)",
        "tor": "Tooro",
        "tou": "Toubou",
        "tpi": "Tok Pisin",
        "tra": "Transnistrians",
        "tri": "Tripuri",
        "trn": "Ternate",
        "tsi": "Tsimshian",
        "tsn": "Tswana",
        "tso": "Tsonga",
        "tts": "Tutsi",
        "tuj": "Tujia",
        "tuk": "Turkmen",
        "tum": "Tumbuka",
        "tup": "Tupi",
        "tur": "Turkish",
        "tuu": "Mongour",
        "tvl": "Tuvaluans",
        "twi": "Ashanti",
        "twn": "Taiwanese",
        "tyv": "Tuvans",
        "udm": "Udmurt",
        "uig": "Uyghur",
        "ukr": "Ukranian",
        "umb": "Southern Mbundu",
        "und": "Undetermined",
        "urd": "Urdu",
        "uzb": "Uzbeks",
        "vaa": "Va",
        "vai": "Vai",
        "ven": "Venda",
        "vie": "Vietnamese",
        "vil": "Vili",
        "vnz": "Venezuelan",
        "vot": "Votes",
        "wak": "Wakashan",
        "wal": "Welayta",
        "war": "Waray",
        "was": "Washoe",
        "wel": "Welsh",
        "wen": "Sorbs",
        "whi": "Whites",
        "wln": "Walloons",
        "wol": "Wolof",
        "xal": "Kalmyk",
        "xho": "Xhosa",
        "xib": "Xibe",
        "xnc": "Xinca",
        "yao": "Yao",
        "yap": "Yapese",
        "yor": "Yoruba",
        "ypk": "Yupik",
        "yug": "Yugur",
        "zag": "Zaghawa",
        "zap": "Zapotec",
        "zay": "Zaidiyya",
        "zen": "Zenaga",
        "zha": "Zhuang",
        "znd": "Azande",
        "zom": "Zomi",
        "zor": "Zoroastrians",
        "zul": "Zulu",
        "zun": "Zuni",
        "zza": "Zaza"
    }

    # from https://www.gdeltproject.org/data/lookups/CAMEO.religion.txt
    religion_codes = {
        "ADR": "African Diasporic Religion",
        "ALE": "Alewi",
        "ATH": "Agnostic",
        "BAH": "Bahai Faith",
        "BUD": "Buddhism",
        "CHR": "Christianity",
        "CON": "Confucianism",
        "CPT": "Coptic",
        "CTH": "Catholic",
        "DOX": "Orthodox",
        "DRZ": "Druze",
        "HIN": "Hinduism",
        "HSD": "Hasidic",
        "ITR": "Indigenous Tribal Religion",
        "JAN": "Jainism",
        "JEW": "Judaism",
        "JHW": "Jehovah's Witness",
        "LDS": "Latter Day Saints",
        "MOS": "Muslim",
        "MRN": "Maronite",
        "NRM": "New Religious Movement",
        "PAG": "Pagan",
        "PRO": "Protestant",
        "SFI": "Sufi",
        "SHI": "Shia",
        "SHN": "Old Shinto School",
        "SIK": "Sikh",
        "SUN": "Sunni",
        "TAO": "Taoist",
        "UDX": "Ultra-Orthodox",
        "ZRO": "Zoroastrianism"
    }

    # from https://www.gdeltproject.org/data/lookups/CAMEO.country.txt
    country_codes = {
        "WSB": "West Bank",
        "BAG": "Baghdad",
        "GZS": "Gaza Strip",
        "AFR": "Africa",
        "ASA": "Asia",
        "BLK": "Balkans",
        "CRB": "Caribbean",
        "CAU": "Caucasus",
        "CFR": "Central Africa",
        "CAS": "Central Asia",
        "CEU": "Central Europe",
        "EIN": "East Indies",
        "EAF": "Eastern Africa",
        "EEU": "Eastern Europe",
        "EUR": "Europe",
        "LAM": "Latin America",
        "MEA": "Middle East",
        "MDT": "Mediterranean",
        "NAF": "North Africa",
        "NMR": "North America",
        "PGS": "Persian Gulf",
        "SCN": "Scandinavia",
        "SAM": "South America",
        "SAS": "South Asia",
        "SEA": "Southeast Asia",
        "SAF": "Southern Africa",
        "WAF": "West Africa",
        "WST": "The West",
        "AFG": "Afghanistan",
        "ALA": "Aland Islands",
        "ALB": "Albania",
        "DZA": "Algeria",
        "ASM": "American Samoa",
        "AND": "Andorra",
        "AGO": "Angola",
        "AIA": "Anguilla",
        "ATG": "Antigua and Barbuda",
        "ARG": "Argentina",
        "ARM": "Armenia",
        "ABW": "Aruba",
        "AUS": "Australia",
        "AUT": "Austria",
        "AZE": "Azerbaijan",
        "BHS": "Bahamas",
        "BHR": "Bahrain",
        "BGD": "Bangladesh",
        "BRB": "Barbados",
        "BLR": "Belarus",
        "BEL": "Belgium",
        "BLZ": "Belize",
        "BEN": "Benin",
        "BMU": "Bermuda",
        "BTN": "Bhutan",
        "BOL": "Bolivia",
        "BIH": "Bosnia and Herzegovina",
        "BWA": "Botswana",
        "BRA": "Brazil",
        "VGB": "British Virgin Islands",
        "BRN": "Brunei Darussalam",
        "BGR": "Bulgaria",
        "BFA": "Burkina Faso",
        "BDI": "Burundi",
        "KHM": "Cambodia",
        "CMR": "Cameroon",
        "CAN": "Canada",
        "CPV": "Cape Verde",
        "CYM": "Cayman Islands",
        "CAF": "Central African Republic",
        "TCD": "Chad",
        "CHL": "Chile",
        "CHN": "China",
        "COL": "Columbia",
        "COM": "Comoros",
        "COD": "Democratic Republic of the Congo",
        "COG": "People's Republic of the Congo",
        "COK": "Cook Islands",
        "CRI": "Costa Rica",
        "CIV": "Ivory Coast",
        "HRV": "Croatia",
        "CUB": "Cuba",
        "CYP": "Cyprus",
        "CZE": "Czech Republic",
        "DNK": "Denmark",
        "DJI": "Djibouti",
        "DMA": "Dominica",
        "DOM": "Dominican Republic",
        "TMP": "East Timor",
        "ECU": "Ecuador",
        "EGY": "Egypt",
        "SLV": "El Salvador",
        "GNQ": "Equatorial Guinea",
        "ERI": "Eritrea",
        "EST": "Estonia",
        "ETH": "Ethiopia",
        "FRO": "Faeroe Islands",
        "FLK": "Falkland Islands",
        "FJI": "Fiji",
        "FIN": "Finland",
        "FRA": "France",
        "GUF": "French Guiana",
        "PYF": "French Polynesia",
        "GAB": "Gabon",
        "GMB": "Gambia",
        "GEO": "Georgia",
        "DEU": "Germany",
        "GHA": "Ghana",
        "GIB": "Gibraltar",
        "GRC": "Greece",
        "GRL": "Greenland",
        "GRD": "Grenada",
        "GLP": "Guadeloupe",
        "GUM": "Guam",
        "GTM": "Guatemala",
        "GIN": "Guinea",
        "GNB": "Guinea-Bissau",
        "GUY": "Guyana",
        "HTI": "Haiti",
        "VAT": "Vatican City",
        "HND": "Honduras",
        "HKG": "Hong Kong",
        "HUN": "Hungary",
        "ISL": "Iceland",
        "IND": "India",
        "IDN": "Indonesia",
        "IRN": "Iran",
        "IRQ": "Iraq",
        "IRL": "Ireland",
        "IMY": "Isle of Man",
        "ISR": "Israel",
        "ITA": "Italy",
        "JAM": "Jamaica",
        "JPN": "Japan",
        "JOR": "Jordan",
        "KAZ": "Kazakhstan",
        "KEN": "Kenya",
        "KIR": "Kiribati",
        "PRK": "North Korea",
        "KOR": "South Korea",
        "KWT": "Kuwait",
        "KGZ": "Kyrgyzstan",
        "LAO": "Laos",
        "LVA": "Latvia",
        "LBN": "Lebanon",
        "LSO": "Lesotho",
        "LBR": "Liberia",
        "LBY": "Libya",
        "LIE": "Liechtenstein",
        "LTU": "Lithuania",
        "LUX": "Luxembourg",
        "MAC": "Macao",
        "MKD": "Macedonia",
        "MDG": "Madagascar",
        "MWI": "Malawi",
        "MYS": "Malaysia",
        "MDV": "Maldives",
        "MLI": "Mali",
        "MLT": "Malta",
        "MHL": "Marshall Islands",
        "MTQ": "Martinique",
        "MRT": "Mauritania",
        "MUS": "Mauritius",
        "MYT": "Mayotte",
        "MEX": "Mexico",
        "FSM": "Micronesia",
        "MDA": "Moldova",
        "MCO": "Monaco",
        "MNG": "Mongolia",
        "MTN": "Montenegro",
        "MSR": "Montserrat",
        "MAR": "Morocco",
        "MOZ": "Mozambique",
        "MMR": "Myanmar",
        "NAM": "Namibia",
        "NRU": "Nauru",
        "NPL": "Nepal",
        "NLD": "Netherlands",
        "ANT": "Netherlands Antilles",
        "NCL": "New Caledonia",
        "NZL": "New Zealand",
        "NIC": "Nicaragua",
        "NER": "Niger",
        "NGA": "Nigeria",
        "NIU": "Niue",
        "NFK": "Norfolk Island",
        "MNP": "Northern Mariana Islands",
        "NOR": "Norway",
        "PSE": "Occupied Palestinian Territory",
        "OMN": "Oman",
        "PAK": "Pakistan",
        "PLW": "Palau",
        "PAN": "Panama",
        "PNG": "Papua New Guinea",
        "PRY": "Paraguay",
        "PER": "Peru",
        "PHL": "Philippines",
        "PCN": "Pitcairn",
        "POL": "Poland",
        "PRT": "Portugal",
        "PRI": "Puerto Rico",
        "QAT": "Qatar",
        "REU": "Runion",
        "ROM": "Romania",
        "RUS": "Russia",
        "RWA": "Rwanda",
        "SHN": "Saint Helena",
        "KNA": "Saint Kitts-Nevis",
        "LCA": "Saint Lucia",
        "SPM": "Saint Pierre and Miquelon",
        "VCT": "Saint Vincent and the Grenadines",
        "WSM": "Samoa",
        "SMR": "San Marino",
        "STP": "Sao Tome and Principe",
        "SAU": "Saudi Arabia",
        "SEN": "Senegal",
        "SRB": "Serbia",
        "SYC": "Seychelles",
        "SLE": "Sierra Leone",
        "SGP": "Singapore",
        "SVK": "Slovakia",
        "SVN": "Slovenia",
        "SLB": "Solomon Islands",
        "SOM": "Somalia",
        "ZAF": "South Africa",
        "ESP": "Spain",
        "LKA": "Sri Lanka",
        "SDN": "Sudan",
        "SUR": "Suriname",
        "SJM": "Svalbard and Jan Mayen Islands",
        "SWZ": "Swaziland",
        "SWE": "Sweden",
        "CHE": "Switzerland",
        "SYR": "Syria",
        "TWN": "Taiwan",
        "TJK": "Tajikistan",
        "TZA": "Tanzania",
        "THA": "Thailand",
        "TGO": "Togo",
        "TKL": "Tokelau",
        "TON": "Tonga",
        "TTO": "Trinidad and Tobago",
        "TUN": "Tunisia",
        "TUR": "Turkey",
        "TKM": "Turkmenistan",
        "TCA": "Turks and Caicos Islands",
        "TUV": "Tuvalu",
        "UGA": "Uganda",
        "UKR": "Ukraine",
        "ARE": "United Arab Emirates",
        "GBR": "United Kingdom",
        "USA": "United States",
        "VIR": "United States Virgin Islands",
        "URY": "Uruguay",
        "UZB": "Uzbekistan",
        "VUT": "Vanuatu",
        "VEN": "Venezuela",
        "VNM": "Vietnam",
        "WLF": "Wallis and Futuna Islands",
        "ESH": "Western Sahara",
        "YEM": "Yemen",
        "ZMB": "Zambia",
        "ZWE": "Zimbabwe"
    }

    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.actor_type_decoder = DecodingValueExtractor(self.actor_codes,
                                                         'Actor Code Decoder')
        self.known_group_decoder = DecodingValueExtractor(self.known_group_codes,
                                                          'Known Groups Decoder')
        self.ethnic_group_decoder = DecodingValueExtractor(self.known_group_codes,
                                                           'Ethnic Groups Decoder')
        self.religion_decoder = DecodingValueExtractor(self.religion_codes,
                                                       'Religion Decoder')
        self.country_decoder = DecodingValueExtractor(self.country_codes,
                                                      'Country Decoder')

    def document_selector(self, doc: Document) -> bool:
        return doc.cdr_document.get("dataset") == "gdelt-actor"

    def process_document(self, doc: Document) -> List[Document]:
        doc.kg.add_value("title", json_path="$.ActorName")

        # Type
        doc.kg.add_value("type", value="Actor")
        doc.kg.add_value("type", json_path="$.ActorType1Code")
        doc.kg.add_value("type", value=doc.extract(self.actor_type_decoder, doc.select_segments("$.ActorType1Code")[0]))
        doc.kg.add_value("type", json_path="$.ActorType2Code")
        doc.kg.add_value("type", value=doc.extract(self.actor_type_decoder, doc.select_segments("$.ActorType2Code")[0]))
        doc.kg.add_value("type", json_path="$.ActorType3Code")
        doc.kg.add_value("type", value=doc.extract(self.actor_type_decoder, doc.select_segments("$.ActorType2Code")[0]))

        # Ethnic group
        doc.kg.add_value("ethnic_group", json_path="$.ActorEthnicCode")
        doc.kg.add_value("ethnic_group",
                         value=doc.extract(self.ethnic_group_decoder, doc.select_segments("$.ActorEthnicCode")[0]))

        # Religion
        doc.kg.add_value("religion", json_path="$.ActorReligion1Code")
        doc.kg.add_value("religion", value=doc.extract(self.religion_decoder, doc.select_segments("$.ActorReligion1Code")[0]))
        doc.kg.add_value("religion", json_path="$.ActorReligion2Code")
        doc.kg.add_value("religion", value=doc.extract(self.religion_decoder, doc.select_segments("$.ActorReligion2Code")[0]))

        # Known group: putting as label
        doc.kg.add_value("label", json_path="$.ActorKnownGroupCode")
        doc.kg.add_value("label",
                         value=doc.extract(self.known_group_decoder, doc.select_segments("$.ActorKnownGroupCode")[0]))

        # Country, refers to the affiliation, being mapped to country of actor, losing the distinction.
        doc.kg.add_value("country", json_path="$.ActorCountryCode")
        doc.kg.add_value("country",
                         value=doc.extract(self.country_decoder, doc.select_segments("$.ActorCountryCode")[0]))

        # Note: not mapping the Actor Geo codes, because Pedro doesn't understand what they mean.
        return list()


if __name__ == "__main__":

    # Tell ETK the schema of the fields in the KG, the DIG master_config can be used as the schema.
    kg_schema = KGSchema(json.load(open('../events_ucdp/master_config.json')))

    # Instantiate ETK, with the two processing modules and the schema.
    etk = ETK(modules=[GdeltModule, GdeltActorModule], kg_schema=kg_schema)

    # Create a CSV processor to create documents for the relevant rows in the TSV file
    cp = CsvProcessor(etk=etk,
                      heading_columns=(1, len(GdeltModule.header_fields)),
                      column_name_prefix="COL")

    with open("gdelt.jl", "w") as f:
        # Iterate over all the rows in the spredsheet
        for d in cp.tabular_extractor(filename="20170912.export_sample.tsv", dataset='gdelt'):
            for result in etk.process_ems(d):
                # print(d.cdr_document)
                # print(json.dumps(result.cdr_document.get("knowledge_graph"), indent=2))
                print(result.cdr_document.get("knowledge_graph"))
                f.write(json.dumps(result.cdr_document) + "\n")
