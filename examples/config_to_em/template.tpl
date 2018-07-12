import re

from etk.etk_module import ETKModule
from etk.document_selector import DefaultDocumentSelector
from etk.document import Document

from etk.extractors.html_content_extractor import HTMLContentExtractor, Strategy
from etk.extractors.html_metadata_extractor import HTMLMetadataExtractor
from etk.extractors.inferlink_extractor import InferlinkRuleSet, InferlinkExtractor

from etk.extractors.glossary_extractor import GlossaryExtractor
from etk.extractors.spacy_rule_extractor import SpacyRuleExtractor

from etk.extractors.bitcoin_address_extractor import BitcoinAddressExtractor
from etk.extractors.cryptographic_hash_extractor import CryptographicHashExtractor
from etk.extractors.cve_extractor import CVEExtractor
from etk.extractors.date_extractor import DateExtractor
from etk.extractors.hostname_extractor import HostnameExtractor
from etk.extractors.ip_address_extractor import IPAddressExtractor
from etk.extractors.table_extractor import TableExtractor
from etk.extractors.url_extractor import URLExtractor

class BaseETKModule(ETKModule):
    """
    Abstract class for extraction module
    """
    def __init__(self, etk):
        ETKModule.__init__(self, etk)
        self.readability_extractor = HTMLContentExtractor()
        self.meta_extractor = HTMLMetadataExtractor()
        self.inferlink_extractor = InferlinkExtractor(InferlinkRuleSet(InferlinkRuleSet.load_rules_file('sample_inferlink_rules.json')))
        self.master_config_fields = etk.kg_schema.fields_dict

${extractor_list}

    def process_document(self, doc: Document):
        """
        Add your code for processing the document
        """
        doc.value["readability_extraction"] = dict()

        all_text = doc.extract(self.readability_extractor, doc.select_segments("$.raw_content")[0], strategy=Strategy.ALL_TEXT)
        doc.select_segments("$.readability_extraction")[0].store(all_text, "all_text")

        strict_text = doc.extract(self.readability_extractor, doc.select_segments("$.raw_content")[0], strategy=Strategy.MAIN_CONTENT_STRICT)
        doc.select_segments("$.readability_extraction")[0].store(strict_text, "strict_text")

        relax_text = doc.extract(self.readability_extractor, doc.select_segments("$.raw_content")[0], strategy=Strategy.MAIN_CONTENT_RELAXED)
        doc.select_segments("$.readability_extraction")[0].store(relax_text, "relax_text")

        if strict_text and 'description' in self.master_config_fields:
            doc.kg.add_value('description', strict_text[0].value)

        meta = self.meta_extractor.extract(doc.cdr_document["raw_content"], extract_title=True)
        for e in meta:
            if e.tag == 'title' and e.tag in self.master_config_fields:
                doc.kg.add_value(e.tag, value=e.value)

        inferlink_extractions = self.inferlink_extractor.extract(doc.cdr_document["raw_content"])
        for e in inferlink_extractions:
            field_name = re.sub(r'-\d$', '', e.tag)
            if field_name in self.master_config_fields:
                doc.kg.add_value(field_name, value=e.value)

        if 'website' in doc.value:
            doc.kg.add_value('website', value=doc.value['website'])
        elif 'url' in doc.value:
            try:
                website = HostnameExtractor().extract(doc.value['url'])
                if website:
                    doc.kg.add_value('website', value=website[0].value)
            except Exception as e:
                print(e)

        for text in all_text:
${execution_list}

        for text in strict_text:
${execution_list}

        for text in relax_text:
${execution_list}

    def document_selector(self, doc: Document) -> bool:
        """
        Boolean function for selecting document
        Args:
            doc: Document

        Returns:

        """
        return DefaultDocumentSelector().select_document(doc)
