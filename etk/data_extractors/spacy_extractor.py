#import spacy
#nlp = spacy.load('en')

def spacy_extract(text_string):
    out = dict()
    doc = nlp(text_string)
    for ent in doc.ents:
        if ent.label_ not in out:
            out[ent.label_] = list()
        out[ent.label_].append(ent.text)
    return out
