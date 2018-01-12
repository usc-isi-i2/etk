"""
Fields to remove
raw_content
objects[].response_headers
content_extraction
response_headers
crawl_data
extractions
content_type
extracted_metadata
extracted_text
knowledge_graph.states_usa_codes
knowledge_graph.city_name
knowledge_graph.populated_places
knowledge_graph.location
knowledge_graph.*.provenance.context.start
knowledge_graph.*.provenance.context.end
knowledge_graph.*.provenance.context.input
knowledge_graph.*.provenance.confidence
"""


def slim_etk_out(doc):
    doc.pop('crawl_data', None)
    doc.pop('extractions', None)
    doc.pop('raw_content', None)
    doc.pop('response_headers', None)
    doc.pop('content_extraction', None)
    doc.pop('content_type', None)
    doc.pop('extracted_metadata', None)
    doc.pop('extracted_text', None)
    doc.pop('@execution_profile', None)

    if 'objects' in doc:
        objects = doc['objects']
        if not isinstance(objects, list):
            objects = [objects]
        for object in objects:
            object.pop('response_headers', None)
        doc['objects'] = objects

    doc['knowledge_graph'] = clean_kg(doc['knowledge_graph'])
    return doc


def clean_kg(knowledge_graph):
    knowledge_graph.pop('states_usa_codes', None)
    knowledge_graph.pop('city_name', None)
    knowledge_graph.pop('populated_places', None)
    knowledge_graph.pop('location', None)

    for key in knowledge_graph.keys():
        extractions = knowledge_graph[key]
        for extraction in extractions:
            provs = extraction['provenance']
            if not isinstance(provs, list):
                provs = [provs]
            for prov in provs:
                prov.pop('confidence', None)
                if 'source' in prov and 'context' in prov['source']:
                    context = prov['source']['context']
                    context.pop('input', None)
                    context.pop('start', None)
                    context.pop('end', None)
                    prov['source']['context'] = context
            extraction['provenance'] = provs
        knowledge_graph[key] = extractions
    return knowledge_graph


if __name__ == "__main__":
    import json
    import codecs
    f = codecs.open('/tmp/etk_sample.jl')
    o = codecs.open('/tmp/slim_etk_sample.jl', 'w')
    for line in f:
        x = json.loads(line)
        o.write(json.dumps(slim_etk_out(x)))
        o.write('\n')