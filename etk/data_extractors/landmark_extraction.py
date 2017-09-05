from landmark_extractor.extraction.Landmark import Rule, RuleSet, flattenResult


def extract(html, url, extractionrulesall, threshold=0.5):
    inferlink_extraction = dict()
    extractionrules_list = None
    number_of_rules = 0
    matched_rule_key = None
    for rule_key in extractionrulesall.keys():
        if rule_key in url:
            # print rule_key, url
            extractionrules_list = extractionrulesall[rule_key]
            number_of_rules = len(extractionrules_list)
            matched_rule_key = rule_key
            break

    try:
        if extractionrules_list:
            for extractionrules_obj in extractionrules_list:
                extractionrules = extractionrules_obj['rules']
                rules = RuleSet(extractionrules)
                if rules is not None:
                    extraction_list = rules.extract(html)
                    flatten = flattenResult(extraction_list)
                    if flatten:
                        if "posts-0-0" in flatten:
                            # this is a forum kind of page, handle differently
                            if len(flatten['posts-0-0']) > 0:
                                return flatten['posts-0-0']
                        elif "posts" in flatten:
                            # this is a forum kind of page, handle differently
                            if len(flatten['posts']) > 0:
                                return flatten['posts']
                        else:
                            for key in flatten.keys():
                                if flatten[key].strip() != '':
                                    inferlink_extraction[key] = flatten[key]
                properly_extracted_fields = len(inferlink_extraction)
                if not (properly_extracted_fields > 0 and float(properly_extracted_fields) / float(number_of_rules) >= threshold):
                    # print 'rules  %s succeeded for  %s' % (matched_rule_key, url)
                    # print '%s rules matched out of %s' % (properly_extracted_fields, number_of_rules)
                    # print inferlink_extraction
                    inferlink_extraction = dict()
                # else:
                #     # if matched_rule_key:
                #     #     print 'rules  %s failed for  %s' % (matched_rule_key, url)
                #     #     print '%s rules matched out of %s' % (properly_extracted_fields, number_of_rules)
                #     return None

        return inferlink_extraction if len(inferlink_extraction.keys()) > 0 else None
    except Exception, e:
        print "ERRROR:", str(e)
    return None