from spacy.matcher import Matcher
from spacy.attrs import LIKE_EMAIL

filter_lst = ["noon", "no"]


def load_like_email_matcher(nlp):
    matcher = Matcher(nlp.vocab)
    matcher.add_pattern(1,
                        [
                            {LIKE_EMAIL: True}
                        ])
    return matcher


def check_domain(doc):
    idx = None
    for e in doc:
        if e.text == "@":
            idx = e.i
            break
    if not idx or doc[idx+1].text in filter_lst:
        return False
    else:
        return True


def get_non_space_email(doc, new_doc, new_nlp, t):
    result_lst = []
    for e in doc:
        if "email:" in e.text.lower():
            value = e.text[6:]
            tt = [i["value"] for i in t.extract(value, lowercase=False)]
            dd = new_nlp(tt)
            for i in range(len(new_doc)-len(dd)):
                j = i+len(dd)
                if new_doc[i:j].text.strip() == dd.text.strip():
                    break
            result = {
                "value": value,
                "context": {
                    "start": i,
                    "end": j,
                }
            }
            result_lst.append(result)
        elif "e-mail:" in e.text.lower():
            value = e.text[7:]
            tt = [i["value"] for i in t.extract(value, lowercase=False)]
            dd = new_nlp(tt)
            for i in range(len(new_doc) - len(dd)):
                j = i + len(dd)
                if doc[i:j].text.strip() == dd.text.strip():
                    break
            result = {
                "value": value,
                "context": {
                    "start": i,
                    "end": j,
                }
            }
            result_lst.append(result)
    return result_lst


def extract(text, nlp, new_nlp, t):
    matcher = load_like_email_matcher(nlp)
    spacy_doc = nlp(text, parse=False)
    new_token = [i["value"] for i in t.extract(text, lowercase=False)]
    new_doc = new_nlp(new_token, parse=False)
    non_space_email = get_non_space_email(spacy_doc, new_doc, new_nlp, t)
    like_email_matches = matcher(spacy_doc)
    filtered_lst = []
    for (ent, label, start, end) in like_email_matches:
        flag = True
        this_text = spacy_doc[start:end].text
        this_crf_tokens = [i["value"] for i in t.extract(this_text, lowercase=False)]
        crf_spacy_doc = new_nlp(this_crf_tokens, parse=False)
        if crf_spacy_doc[-1].is_digit:
            flag = False
        if flag:
            flag = check_domain(crf_spacy_doc)
        if flag:
            for i in range(len(new_doc)-len(crf_spacy_doc)):
                j = i+len(crf_spacy_doc)
                if new_doc[i:j].text.strip() == crf_spacy_doc.text.strip():
                    break
            result = {
                "value": this_text,
                "context": {
                    "start": i,
                    "end": j,
                }
            }
            filtered_lst.append(result)
    return filtered_lst + non_space_email
