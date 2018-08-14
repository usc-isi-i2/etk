import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from etk.ontology_api import Ontology
from etk.ontology_report_generator import OntologyReportGenerator

dir_path = os.path.dirname(os.path.abspath(__file__))
input_turtle = dir_path + "/../../ontologies/default-ontology.ttl"
output_doc = dir_path + "/default-ontology.html"
doc_content = ""

with open(input_turtle) as f:
    ontology = Ontology(f.read(), validation=False, include_undefined_class=True)
    doc_content = OntologyReportGenerator(ontology).generate_html_report(include_turtle=True)

with open(output_doc, "w") as f:
    f.write(doc_content)
