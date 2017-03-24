from landmark_extractor.extraction.Landmark import Rule, RuleSet, flattenResult


def landmark_extractor(html, url, extractionrulesall, threshold=0.5):
    inferlink_extraction = dict()
    extractionrules = None
    number_of_rules = 0
    matched_rule_key = None
    for rule_key in extractionrulesall.keys():
        if rule_key in url:
            extractionrules = extractionrulesall[rule_key]
            number_of_rules = len(extractionrules)
            matched_rule_key = rule_key
            break

    try:
        rules = RuleSet(extractionrules)
        if rules is not None:
            extraction_list = rules.extract(html)
            flatten = flattenResult(extraction_list)
            for key in flatten.keys():
                if flatten[key].strip() != '':
                    inferlink_extraction[key] = flatten[key]
        properly_extracted_fields = len(inferlink_extraction)
        if properly_extracted_fields > 0 and float(properly_extracted_fields) / float(number_of_rules) >= threshold:
            print 'rules  %s succeeded for  %s' % (matched_rule_key, url)
            print '%s rules matched out of %s' % (properly_extracted_fields, number_of_rules)
            return inferlink_extraction
        else:
            if matched_rule_key:
                print 'rules  %s failed for  %s' % (matched_rule_key, url)
                print '%s rules matched out of %s' % (properly_extracted_fields, number_of_rules)
            return None
    except Exception, e:
        print "ERRROR:", str(e)
    return None