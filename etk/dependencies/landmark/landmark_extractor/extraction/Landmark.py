import sys
import getopt
import json
import regex as re
import abc
import codecs
import cgi
import uuid
from etk.dependencies.landmark.landmark_extractor.postprocessing.PostProcessor import RemoveExtraSpaces, RemoveHtml
# from landmark_extractor.postprocessing.PostProcessor import RemoveExtraSpaces, RemoveHtml
from collections import OrderedDict

MAX_EXTRACT_LENGTH=100000
ITEM_RULE = 'ItemRule'
ITERATION_RULE = 'IterationRule'
#from http://www.fon.hum.uva.nl/praat/manual/Regular_expressions_1__Special_characters.html
_specialcharacters = frozenset(
    '\^${}[]().*+?|<>-&')

STATUS_INVALID = 'INVALID'
STATUS_VALID = 'VALID'
DIGIT_VALIDATION_REGEX = '^\\d+$'
NOT_NULL_VALIDATION_REGEX = '^.*?$'

def escape_regex_string(input_string):
    s = list(input_string)
    specialcharacters = _specialcharacters
    for i, c in enumerate(input_string):
        if c in specialcharacters:
            if c == "\000":
                s[i] = "\\000"
            else:
                s[i] = '\\' + c
    return input_string[:0].join(s)

def flattenResult(extraction_object, name = 'root'):
    result = {}
    if isinstance(extraction_object, dict):
        if 'sub_rules' in extraction_object:
            for item in extraction_object['sub_rules']:
                result[item] = flattenResult(extraction_object['sub_rules'][item], item)
        elif 'sequence' in extraction_object:
            result = flattenResult(extraction_object['sequence'], 'sequence')
        elif 'extract' in extraction_object:
            return extraction_object['extract']
        else:
            for extract in extraction_object:
                result[extract] = flattenResult(extraction_object[extract], extract)
    
    if isinstance(extraction_object, list):
        result = []
        for extract in extraction_object:
            result.append(flattenResult(extract, 'sequence'))
    return result

def loadRule(rule_json_object):
    """ Method to load the rules - when adding a new rule it must be added to the if statement within this method. """
    name = rule_json_object['name']
    rule_type = rule_json_object['rule_type']
    validation_regex = None
    required = False
    removehtml = False
    include_end_regex = False #Default to false for bakward compatibility
    strip_end_regex = None
    sub_rules = []
    begin_stripe_id = None
    end_stripe_id = None
    begin_shift = 0
    end_shift = 0

    if 'sub_rules' in rule_json_object:
        sub_rules = rule_json_object['sub_rules']
    
    if 'validation_regex' in rule_json_object:
        validation_regex = rule_json_object['validation_regex']
    
    if 'required' in rule_json_object:
        required = rule_json_object['required']
    
    if 'removehtml' in rule_json_object:
        removehtml = rule_json_object['removehtml']
        
    if 'include_end_regex' in rule_json_object:
        include_end_regex = rule_json_object['include_end_regex']
    
    if 'strip_end_regex' in rule_json_object:
        strip_end_regex = rule_json_object['strip_end_regex']

    if 'begin_stripe_id' in rule_json_object:
        begin_stripe_id = rule_json_object['begin_stripe_id']

    if 'end_stripe_id' in rule_json_object:
        end_stripe_id = rule_json_object['end_stripe_id']

    if 'begin_shift' in rule_json_object:
        begin_shift = rule_json_object['begin_shift']

    if 'end_shift' in rule_json_object:
        end_shift = rule_json_object['end_shift']

    rule = {}
    
    """ This is where we add our new type """
    if rule_type == ITEM_RULE or rule_type == 'RegexRule':
        begin_regex = rule_json_object['begin_regex']
        end_regex = rule_json_object['end_regex']
        rule = ItemRule(name, begin_regex, end_regex, include_end_regex, strip_end_regex, validation_regex, required,
                        removehtml, sub_rules, begin_stripe_id, end_stripe_id, begin_shift, end_shift)
    if rule_type == ITERATION_RULE or rule_type == 'RegexIterationRule':
        begin_regex = rule_json_object['begin_regex']
        end_regex = rule_json_object['end_regex']
        iter_begin_regex = rule_json_object['iter_begin_regex']
        iter_end_regex = rule_json_object['iter_end_regex']
        no_first_begin_iter_rule = False
        if 'no_first_begin_iter_rule' in rule_json_object:
            no_first_begin_iter_rule = rule_json_object['no_first_begin_iter_rule']
        no_last_end_iter_rule = False
        if 'no_last_end_iter_rule' in rule_json_object:
            no_last_end_iter_rule = rule_json_object['no_last_end_iter_rule']
        
        rule = IterationRule(name, begin_regex, end_regex, iter_begin_regex, iter_end_regex,
                                  include_end_regex, strip_end_regex, no_first_begin_iter_rule,
                                  no_last_end_iter_rule, validation_regex, required, removehtml,
                                  sub_rules, begin_shift=begin_shift, end_shift=end_shift)

    if 'id' in rule_json_object:
        rule.id = rule_json_object['id']
    
    return rule
    
class Rule:
    """ Base abstract class for all rules """
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def apply(self, page_string):
        """ Method to apply this rule. The output is a JSON object of the extraction"""
        
    @abc.abstractmethod
    def toolTip(self):
        """ Method that produces an HTML tooltop for this rule """
    
    @abc.abstractmethod
    def toJson(self):
        """ Method to print the rule as JSON """
        
    @abc.abstractmethod
    def validate(self, value):
        """ Method to validate this rule """
    
    def remove_html(self, value):
        if self.removehtml:
            processor = RemoveHtml(value)
            value = processor.post_process()
            
        processor = RemoveExtraSpaces(value)
        value = processor.post_process()
        return value
    
    def set_name(self, name):
        self.name = name
    
    def set_sub_rules(self, sub_rules):
        self.sub_rules = sub_rules
    
    def set_visible_chunk_before(self, visible_chunk_before):
        self.visible_chunk_before = visible_chunk_before
        
    def set_visible_chunk_after(self, visible_chunk_after):
        self.visible_chunk_after = visible_chunk_after
    
    def __init__(self, name, validation_regex = None, required = False, removehtml = False, sub_rules = None):
        self.id = str(uuid.uuid4())
        self.name = name
        self.validation_regex = None
        if validation_regex:
            self.validation_regex = re.compile(validation_regex, re.S)
        self.required = required
        self.removehtml = removehtml 
        self.sub_rules = []
        if sub_rules:
            self.sub_rules = RuleSet(sub_rules)
        self.visible_chunk_before = None
        self.visible_chunk_after = None

class ItemRule(Rule):
    """ Rule to apply a begin set and single end regex and return only ONE value """
    def apply(self, page_string):
        value = self.extract(page_string)
        
        if self.sub_rules:
            value['sub_rules'] = self.sub_rules.extract(value['extract'])
        
        value['extract'] = self.remove_html(value['extract'])
        return value
    
    def extract(self, page_string):
        try:
            begin_match_end = 0
            if self.begin_regex:
                begin_match = self.begin_rule.search(page_string)
                begin_match_end = begin_match.end()
            end_match_start = len(page_string)
            end_match_end = len(page_string)
            
            if self.end_regex:
                end_match = self.end_rule.search(page_string[begin_match_end:])
                end_match_start = end_match.start()
                end_match_end   = end_match.end()
            
            if self.include_end_regex:
                extract = page_string[begin_match_end:begin_match_end+end_match_end]
                begin_index = begin_match_end
                end_index = begin_match_end+end_match_end
            else:
                extract = page_string[begin_match_end:begin_match_end+end_match_start]
                begin_index = begin_match_end
                end_index = begin_match_end+end_match_start
            
            if extract and self.strip_end_regex:
                extract = re.sub(self.strip_end_regex+'$', '', extract)
                end_index = begin_index + len(extract)
        except:
            extract = ''
            begin_index = -1
            end_index = -1
        return {'rule_id': self.id,'extract': extract,'begin_index':begin_index,'end_index':end_index}
    
    def toolTip(self):
        return 'BEGIN RULE: ' + cgi.escape(self.begin_rule.pattern) + '<hr>END RULE: ' + cgi.escape(self.end_rule.pattern)
    
    def toJson(self):
        json_dict = {}
        json_dict['id'] = self.id
        json_dict['name'] = self.name
        json_dict['rule_type'] = ITEM_RULE
        json_dict['begin_regex'] = self.begin_regex
        json_dict['include_end_regex'] = self.include_end_regex
        json_dict['end_regex'] = self.end_regex
        json_dict['strip_end_regex'] = self.strip_end_regex
        json_dict['removehtml'] = self.removehtml
        json_dict['begin_shift'] = self.begin_shift
        json_dict['end_shift'] = self.end_shift
        if self.begin_stripe_id:
            json_dict['begin_stripe_id'] = self.begin_stripe_id
        if self.end_stripe_id:
            json_dict['end_stripe_id'] = self.end_stripe_id
        if self.required:
            json_dict['required'] = self.required
        if self.validation_regex:
            json_dict['validation_regex'] = self.validation_regex.pattern
        if self.visible_chunk_before:
            json_dict['visible_chunk_before'] = self.visible_chunk_before
        if self.visible_chunk_after:
            json_dict['visible_chunk_after'] = self.visible_chunk_after
        if self.sub_rules:
            json_dict['sub_rules'] = json.loads(self.sub_rules.toJson())
        return json.dumps(json_dict)
    
    def validate(self, value):
        if self.validation_regex:
            valid = self.validation_regex.match(value['extract'])
            if not valid:
                value['status'] = STATUS_INVALID
            else:
                value['status'] = STATUS_VALID
        if self.required:
            if value['status'] == STATUS_INVALID or not value['extract']:
                return False
        
        if self.sub_rules:
            valid = self.sub_rules.validate(value['sub_rules'])
            if not valid:
                return False
        return True
    
    def __init__(self, name, begin_regex, end_regex, include_end_regex = False,
                 strip_end_regex = None, validation_regex = None, required = False, removehtml = False,
                 sub_rules = None, begin_stripe_id = None, end_stripe_id = None, begin_shift = 0, end_shift = 0):
        Rule.__init__(self, name, validation_regex, required, removehtml, sub_rules)
        self.begin_rule = re.compile(begin_regex, re.S)
        self.end_rule = re.compile(end_regex, re.S)
        
        self.begin_regex = begin_regex
        self.end_regex = end_regex
        self.include_end_regex = include_end_regex
        self.strip_end_regex = strip_end_regex
        self.begin_stripe_id = begin_stripe_id
        self.end_stripe_id = end_stripe_id
        self.begin_shift = begin_shift
        self.end_shift = end_shift
        
class IterationRule(ItemRule):
    
    """ Rule to apply a begin set and single end regex and return all values """
    def apply(self, page_string):
        base_extract = self.extract(page_string)
        start_page_string = base_extract['extract']
        
        sequence_number = 1
        extracts = []
        start_index = 0
        begin_match_end = -1
        if self.iter_end_regex is not None:
            while start_index < len(start_page_string):
                try:
                    prev_index = start_index
                    if start_index == 0 and self.no_first_begin_iter_rule:
                        begin_match_end = 0
                    else:
                        begin_match = self.iter_begin_rule.search(start_page_string[start_index:])
                        begin_match_end = begin_match.end()

                    end_match = self.iter_end_rule.search(start_page_string[start_index+begin_match_end:])
                    value = start_page_string[start_index+begin_match_end:start_index+begin_match_end+end_match.start()]
                    if 0 < len(value.strip()) < MAX_EXTRACT_LENGTH:
                        extracts.append({'extract':value,'begin_index':start_index+begin_match_end+base_extract['begin_index'],'end_index':start_index+begin_match_end+end_match.start()+base_extract['begin_index'],'sequence_number':sequence_number})
                        sequence_number = sequence_number + 1
                    start_index = start_index+begin_match_end+end_match.start()
                    if prev_index == start_index:
                        start_index+= max(1, end_match.end())
                except:
                    if self.no_last_end_iter_rule and begin_match_end >= 0:
                        end_match_start = len(start_page_string)
                        value = start_page_string[start_index+begin_match_end:start_index+begin_match_end+end_match_start]
                        if 0 < len(value.strip()) < MAX_EXTRACT_LENGTH:
                            extracts.append({'extract':value,'begin_index':start_index+begin_match_end+base_extract['begin_index'],'end_index':start_index+begin_match_end+end_match_start+base_extract['begin_index'],'sequence_number':sequence_number})
                            sequence_number = sequence_number + 1
                    start_index = len(start_page_string)

        elif self.iter_begin_regex:
            while start_index < len(start_page_string):
                try:
                    end_match = self.iter_begin_rule.search(start_page_string[start_index:])
                    value = start_page_string[
                            start_index:start_index + end_match.end()]
                    if 0 < len(value.strip()) < MAX_EXTRACT_LENGTH:
                        extracts.append({'extract': value,
                                         'begin_index': start_index + base_extract['begin_index'],
                                         'end_index': start_index + end_match.end() + base_extract[
                                             'begin_index'], 'sequence_number': sequence_number})
                        sequence_number = sequence_number + 1
                    start_index = start_index + end_match.end()
                except:
                    if self.no_last_end_iter_rule and start_index >= 0:
                        end_match_end = len(start_page_string)
                        value = start_page_string[
                                start_index:start_index + end_match_end]
                        if 0 < len(value.strip()) < MAX_EXTRACT_LENGTH:
                            extracts.append({'extract': value,
                                             'begin_index': start_index + base_extract['begin_index'],
                                             'end_index': start_index + end_match_end +
                                                          base_extract['begin_index'],
                                             'sequence_number': sequence_number})
                            sequence_number = sequence_number + 1
                    start_index = len(start_page_string)
        
        if self.sub_rules:
            for extract in extracts:
                sub_extraction = self.sub_rules.extract(extract['extract'])
                for sub_extract_name in sub_extraction:
                    sub_extraction[sub_extract_name]['begin_index'] += extract['begin_index']
                    sub_extraction[sub_extract_name]['end_index'] += extract['begin_index']
                extract['sub_rules'] = sub_extraction
        
        for extract in extracts:
            extract['extract'] = self.remove_html(extract['extract'])
        
        base_extract['sequence'] = extracts
        base_extract['rule_id'] = self.id
        return base_extract
    
    def toJson(self):
        json_dict = {}
        json_dict['id'] = self.id
        json_dict['name'] = self.name
        json_dict['rule_type'] = ITERATION_RULE
        json_dict['begin_regex'] = self.begin_regex
        json_dict['include_end_regex'] = self.include_end_regex
        json_dict['end_regex'] = self.end_regex
        json_dict['strip_end_regex'] = self.strip_end_regex
        json_dict['iter_begin_regex'] = self.iter_begin_regex
        json_dict['iter_end_regex'] = self.iter_end_regex
        json_dict['no_first_begin_iter_rule'] = self.no_first_begin_iter_rule
        json_dict['no_last_end_iter_rule'] = self.no_last_end_iter_rule
        json_dict['begin_shift'] = self.begin_shift
        json_dict['end_shift'] = self.end_shift
        if self.begin_stripe_id:
            json_dict['begin_stripe_id'] = self.begin_stripe_id
        if self.end_stripe_id:
            json_dict['end_stripe_id'] = self.end_stripe_id
        if self.sub_rules:
            json_dict['sub_rules'] = json.loads(self.sub_rules.toJson())
        return json.dumps(json_dict)
    
    def validate(self, value):
        for single_value in value['sequence']:
            valid = ItemRule.validate(self, single_value)
            if not valid:
                return False
        return True
    
    def __init__(self, name, begin_regex, end_regex, iter_begin_regex,
                 iter_end_regex = None, include_end_regex = False, backwards_end_regex = None,
                 no_first_begin_iter_rule = False, no_last_end_iter_rule = False,
                 validation_regex = None, required = False, removehtml = False, sub_rules = None, begin_shift=0, end_shift=0):
        
        ItemRule.__init__(self, name, begin_regex, end_regex, include_end_regex,
                          backwards_end_regex, validation_regex, required, removehtml, sub_rules,
                          begin_shift=begin_shift, end_shift=end_shift)
        self.iter_begin_regex = iter_begin_regex
        self.iter_end_regex = iter_end_regex
        self.iter_begin_rule = re.compile(iter_begin_regex, re.S)
        if self.iter_end_regex:
            self.iter_end_rule = re.compile(iter_end_regex, re.S)
        self.no_first_begin_iter_rule = no_first_begin_iter_rule
        self.no_last_end_iter_rule = no_last_end_iter_rule

class RuleSet:
    """A set of rules that is built from a JSON Object or JSON Array"""
    def extract(self, page_str):
        extraction_object = OrderedDict()
        for rule in self.rules:
            extraction_object[rule.name] = rule.apply(page_str);
        
        return extraction_object
    
    def validate(self, extraction):
        for rule in self.rules:
            valid = rule.validate(extraction[rule.name])
            if not valid:
                return {}
        return extraction
    
    def names(self):
        names = []
        for rule in self.rules:
            names.append(rule.name)
        return names
    
    def get(self, rule_name):
        for rule in self.rules:
            if rule.name == rule_name:
                return rule
        return None
    
    def add_rule(self, rule):
        self.rules.append(rule)
    
    def toJson(self):
        json_list = []
        for rule in self.rules:
            json_list.append(json.loads(rule.toJson()))
        return json.dumps(json_list, indent=2, separators=(',', ': '))
    
    #Must have over 50 chars and more than 50% html and on 50% of the pages through this one out
    def removeBadRules(self, pages):
        min_length = 50
        percent_with_html_thresh = 0.5
        percent_bad_thresh = 0.5
        good_rules = RuleSet()
        total_pages = len(pages)
        for rule in self.rules:
            previous_removehtml = rule.removehtml
            rule.removehtml = False
            num_bad = 0
            
#             print 'Checking ' + rule.name
            for page_str in pages:
                extraction = rule.apply(page_str)
                extract = extraction['extract']
                original_length = len(extract)
                
                if original_length > min_length and original_length > 0:
                    processor = RemoveHtml(extract)
                    value = processor.post_process()
                    processor = RemoveExtraSpaces(value)
                    value = processor.post_process()
                    new_length = len(value)
                    
                    percentage = new_length/float(original_length)
                    if percentage < percent_with_html_thresh:
#                         print "BAD - " + extract
                        num_bad += 1
                        
            bad_percentage = num_bad/float(total_pages)
            
            if bad_percentage < percent_bad_thresh:
                rule.removehtml = previous_removehtml
                good_rules.add_rule(rule)
#             else:
#                 print bad_percentage
        self.rules = []
        for rule in good_rules.rules:
            self.rules.append(rule)
    
    def __init__(self, json_object=None):
        self.rules = []
        rule_list = []
        if json_object:
            if isinstance(json_object, dict):
                #this is a single rule so add it to the list
                rule_list.append(json_object)
            else:
                rule_list = json_object
                
            for rule_json in rule_list:
                self.rules.append(loadRule(rule_json))

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "fh", ["flatten", "help"])
            
            flatten = False
            
            for opt in opts:
                if opt in [('-f', ''), ('--flatten', '')]:
                    flatten = True
                if opt in [('-h', ''), ('--help', '')]:
                    raise Usage('python extraction/Landmark.py [OPTIONAL_PARAMS] [FILE_TO_EXTRACT] [RULES FILE]\n\t[OPTIONAL_PARAMS]: -f to flatten the result')
                
        except getopt.error as msg:
            raise Usage(msg)
        if len(args) > 1:
                #read the page from arg0
                page_file_str = args[0]
                with codecs.open(page_file_str, "r", "utf-8") as myfile:
                    page_str = myfile.read().encode('utf-8')

                #read the rules from arg1
                json_file_str = args[1]
        else:
            #read the page from stdin
            page_str = sys.stdin.read().encode('utf-8')

            #read the rules from arg0
            json_file_str = args[0]

        with codecs.open(json_file_str, "r", "utf-8") as myfile:
            json_str = myfile.read().encode('utf-8')
        json_object = json.loads(json_str)
        rules = RuleSet(json_object)
        
        extraction_list = rules.extract(page_str)
        extraction_list = rules.validate(extraction_list)
        
        if flatten:
            print(json.dumps(flattenResult(extraction_list), indent=2, separators=(',', ': ')))
        else:
            print(json.dumps(extraction_list, indent=2, separators=(',', ': ')))
        
    except Usage as err:
        print(err.msg, end='', file=sys.stderr, )
        print("for help use --help", end='', file=sys.stderr)
        return 2

if __name__ == "__main__":
    sys.exit(main())