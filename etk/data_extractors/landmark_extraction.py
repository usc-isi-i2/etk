from landmark_extractor.extraction.Landmark import Rule, RuleSet, flattenResult


def landmark_extractor(doc, extractionrulesall):
    if 'raw_content' in doc and doc['raw_content']:
        inferlink_extraction = dict()
        # tld = doc['isi_tld']
        extractionrules = None
        number_of_rules = 0
        url = doc['url']
        matched_rule_key = None
        for rule_key in extractionrulesall.keys():
            if rule_key in url:
                extractionrules = extractionrulesall[rule_key]
                number_of_rules = len(extractionrules)
                matched_rule_key = rule_key
                # if rule_key == 'investorshub.advfn.com/boards/read_msg.aspx':
                #     print 'found %s as rules for %s' % (rule_key, url)
                #     print len(extractionrules)
                break

        try:
            html = doc['raw_content']
            rules = RuleSet(extractionrules)
            if rules is not None:
                extraction_list = rules.extract(html)
                flatten = flattenResult(extraction_list)
                for key in flatten.keys():
                    if flatten[key].strip() != '':
                        inferlink_extraction[key] = flatten[key]
            properly_extracted_fields = len(inferlink_extraction)
            if properly_extracted_fields > 0 and float(properly_extracted_fields) / float(number_of_rules) >= 0.5:
                print 'rules  %s succeeded for  %s' % (matched_rule_key, url)
                print properly_extracted_fields
                print number_of_rules
                return inferlink_extraction
            else:
                if matched_rule_key:
                    print 'rules  %s failed for  %s' % (matched_rule_key, url)
                    print properly_extracted_fields
                    print number_of_rules
                return None
        except Exception, e:
            print "ERRROR:", str(e)
    return None