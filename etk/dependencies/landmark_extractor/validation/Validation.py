import sys
import json
import codecs
import re
import copy
import getopt

#for now contains dummy rules
class ValidationRule:

    def __init__(self, type, param = None):
        if type == "USER_REGEX":
            type = ValidationType.USER_REGEX
        if type == "NOT_NULL":
            type = ValidationType.NOT_NULL
        if type == "IS_URL":
            type = ValidationType.IS_URL
        if type == "MIN_ROWS":
            type = ValidationType.MIN_ROWS
        if type == "MAX_ROWS":
            type = ValidationType.MAX_ROWS
        if type == "RULE1":
            type = ValidationType.RULE1
        if type == "RULE2":
            type = ValidationType.RULE2
        if type == "RULE3":
            type = ValidationType.RULE3
        self.__method = type
        self.__param = param

    def execute_rule(self, field_extraction):
        #print "in execute_rule"
        return self.__method(field_extraction, self.__param)

    #apply user defined regular expreassion on extract
    def user_regex(self, field_extraction, regex):
        #print "in user_regex"
        extract = field_extraction['extract']
        regex = re.compile(regex)
        # apply pattern
        if regex.match(extract):
            # print "matched:", extract
            return True
        else:
            return False

    #check if extract is null or empty string
    def not_null(self, field_extraction, param = None):
        #print "in not_null"
        extract = field_extraction['extract']
        if extract is None or not extract:
            return False
        else:
            return True

    def is_url(self, field_extraction, param = None):
        #print "in is_url"
        extract = field_extraction['extract']
        regex = re.compile(
            #r'^(?:file):///'  # http:// or https://
            r'^(?:http|ftp)s?://'  # http:// or https://
            #r'^((?:http|ftp)s?://)|((?:file):///)' # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)

        #^(?:http|ftp)s?:\/\/(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|localhost|\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(?::\d+)?(?:\/?|[\/?]\S+)$

        if regex.match(extract):
            # print "matched:", extract
            return True
        else:
            return False

    #returns true if the list contains at least min_rows
    def min_rows(self, list_extraction, min_rows):
        #print "in min_rows"
        #this is a list of rows
        #print "list:", list_extraction
        rows = list_extraction['sequence']
        if len(rows) < int(min_rows):
            return False
        else:
            return True

    #returns true if the list contains at most max_rows
    def max_rows(self, list_extraction, max_rows):
        #print "in max_rows"
        #this is a list of rows
        rows = list_extraction['sequence']
        print("rows:", len(rows))
        if len(rows) > int(max_rows):
            return False
        else:
            return True

    def rule1(self, extract, param = None):
        #print "in valid1"
        return True

    def rule2(self, extract, param = None):
        #print "in valid2"
        return True

    def rule3(self, extract, param = None):
        #print "in valid3"
        return False

    def __str__(self):
        output = str('rule:' + self.__method)
        return output


class ValidationType:
    USER_REGEX = ValidationRule.user_regex
    NOT_NULL = ValidationRule.not_null
    IS_URL = ValidationRule.is_url
    MIN_ROWS = ValidationRule.min_rows
    MAX_ROWS = ValidationRule.max_rows
    RULE1 = ValidationRule.rule1
    RULE2 = ValidationRule.rule2
    RULE3 = ValidationRule.rule3

#look at end of file for sample of extraction with validation metadata
#applies validation rules to extracted data and returns extraction json with validation metadata
class Validation:
    def __init__(self, validation_json):
        # validation_json = {"gh56fj78": [see below ...]}]
        # __validation: key = fieldid, value = json array with validation rules
        # [ { "type": "USER_REGEX",
        #     "param": "the regex" },
        #   { "type": "NOT_NULL" },
        #     etc.}
        self.__validation = validation_json

    def get_validations(self):
        return self.__validation

    #applies validation on extraction and outputs extraction with metadata
    def validate_extraction(self, extraction):
        extraction_copy = copy.deepcopy(extraction)
        for page in extraction:
            self.validate_page(extraction_copy[page])
        return extraction_copy

    #validate data on one oage
    def validate_page(self, page_extraction):
        for fieldid in self.__validation:
            self.validate_page_for_field(page_extraction, fieldid, self.__validation[fieldid])
        return page_extraction

    #find field that needs to be validated and validate that field
    def validate_page_for_field(self, page_extraction, field_id, validation_rules):
        #print "page extraction ...", page_extraction
        if isinstance(page_extraction, list):
            # this is a sequence item so we look at the 'sub_rules' if they exist
            for row in page_extraction:
                #print 'row ...', row
                if 'sub_rules' in row:
                    #print "in subrules..."
                    self.validate_page_for_field(row['sub_rules'], field_id, validation_rules)
        elif isinstance(page_extraction, dict):
            #print 'dict ...', page_extraction
            # this is a standard rule so we look at this one itself
            for name in page_extraction:
                #print 'name ...', name
                if 'rule_id' in page_extraction[name] and page_extraction[name]['rule_id'] == field_id:
                    #found field that needs validation
                    self.validate_field(page_extraction[name], validation_rules)
                elif 'sub_rules' in page_extraction[name]:
                    #print "in subrules ..."
                    self.validate_page_for_field(page_extraction[name]['sub_rules'], field_id, validation_rules)
                elif 'sequence' in page_extraction[name]:
                    #print "in sequence..."
                    self.validate_page_for_field(page_extraction[name]['sequence'], field_id, validation_rules)

    #validate one field
    def validate_field(self, field_extraction, validation_rules):
        #print 'validate field ...', field_extraction
        for validation_rule in validation_rules:
            is_valid = self.validate_value(field_extraction, validation_rule)
            #construct validation json to be returned
            validation_rule['valid'] = is_valid
            self.add_validation(field_extraction, validation_rule)

    # validate one field
    def validate_field_extraction(self, field_extraction):
        # print 'validate field ...', field_extraction
        if field_extraction['fieldid'] in self.__validation:
            for validation_rule in self.__validation[field_extraction['fieldid']]:
                is_valid = self.validate_value(field_extraction, validation_rule)
                # construct validation json to be returned
                validation_rule['valid'] = is_valid
                self.add_validation(field_extraction, validation_rule)
        else:
            #no validation for this field
            field_extraction['valid'] = True
            field_extraction['validation'] = None

    #validate given extraction; could be a simple field or a list
    # we handle it differently in the rule execution
    #validation_rule = {type = ValidationType.RULE1, param = param if needed}
    def validate_value(self, field_extraction, validation_rule):
        validation_param = None
        if 'param' in validation_rule:
            validation_param = validation_rule['param']
        one_validation_rule = ValidationRule(validation_rule['type'], validation_param)
        return one_validation_rule.execute_rule(field_extraction)

    #add validation to field
    def add_validation(self, field_extraction, field_validation):
        if 'validation' in field_extraction:
            # already contains some validation; append to it and reset "valid" for this field
            field_extraction['validation'].append(field_validation)
            if field_extraction['valid'] is True and field_validation['valid'] is False:
                field_extraction['valid'] = False
        else:
            validation = []
            validation.append(field_validation)
            field_extraction['validation'] = validation
            field_extraction['valid'] = field_validation['valid']

    #schema_json is an array with one element (root)
    def get_schema_with_validation(self, schema_json):
        #schema_json[0]['list'] contains list of fields
        #a field can be a simple field or another list
        self.add_validation_to_schema(schema_json[0]['list'])
        #print json.dumps(schema_json, sort_keys=True, indent=2, separators=(',', ': '))
        return schema_json

    def add_validation_to_schema(self, schema_field_list):
        for field in schema_field_list:
            if field['schemaid'] in self.__validation:
                # add it
                field['validation'] = self.__validation[field['schemaid']]
            if 'list' in field:
                #this is a list field
                self.add_validation_to_schema(field['list'])

    #validation is either
    # {"valid":true} OR
    # {"valid":false}
    @staticmethod
    def get_validation_for_page_1(page_extraction, validation):
        # if I found something false everything is false
        if validation['valid'] is False:
            return
        if isinstance(page_extraction, list):
            # this is a sequence item so we look at the 'sub_rules' if they exist
            for row in page_extraction:
                #print 'row:',row
                if 'sub_rules' in row:
                    #print "in subrules...."
                    Validation.get_validation_for_page_1(row['sub_rules'], validation)
        elif isinstance(page_extraction, dict):
            #print 'dict ...'
            # this is a standard rule so we look at this one itself
            for name in page_extraction:
                #print 'name ...', name
                if 'valid' in page_extraction[name]:
                    #found field with validation
                    if page_extraction[name]['valid'] is False:
                        validation['valid'] = False
                        #print "False..."
                        return
                if 'sub_rules' in page_extraction[name]:
                    #print "in subrules...."
                    Validation.get_validation_for_page_1(page_extraction[name]['sub_rules'], validation)
                elif 'sequence' in page_extraction[name]:
                    #print "in sequence ..."
                    Validation.get_validation_for_page_1(page_extraction[name]['sequence'], validation)

    #returns either true or false; if it finds one false validation it returns false
    # {"valid":true} OR
    # {"valid":false}
    @staticmethod
    def get_validation_for_extraction(extraction):
        validation = {}
        validation['valid'] = True
        for page in extraction:
            Validation.get_validation_for_page_1(extraction[page], validation)
            if validation['valid'] is False:
                return validation
        return validation

    #returns either true or false; if it finds one false validation it returns false
    # {"valid":true} OR
    # {"valid":false}
    @staticmethod
    def get_validation_for_page(page_extraction):
        validation = {}
        validation['valid'] = True
        Validation.get_validation_for_page_1(page_extraction, validation)
        return validation

    def __str__(self):
        output = str(self.__validation)
        return output

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):

    # with codecs.open("validation_1.json", "r", "utf-8") as myfile:
    #     file_str = myfile.read().encode('utf-8')
    # validation_json = json.loads(file_str)
    # v = Validation(validation_json)
    # print "validation:", v
    #
    # with codecs.open("page_extraction_2.json", "r", "utf-8") as myfile:
    #     page_str = myfile.read().encode('utf-8')
    # page_json = json.loads(page_str)
    # new_page_json = v.validate_extraction(page_json)
    # print "end page:", new_page_json
    # print json.dumps(new_page_json, indent=2, separators=(',', ': '))
    #
    # result = Validation.get_validation_for_extraction(new_page_json)
    # print json.dumps(result, indent=2, separators=(',', ': '))
    # sys.exit()

    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "sh", ["simple", "help"])

            simple = False
            for opt in opts:
                if opt in [('-s', ''), ('--simple', '')]:
                    simple = True
                if opt in [('-h', ''), ('--help', '')]:
                    raise Usage(
                        'python validation/Validation.py [OPTIONAL_PARAMS] [FILE_WITH_VALIDATION] [FILE_WITH_EXTRACT]\n\t[OPTIONAL_PARAMS]: -s to return only true/false')

        except getopt.error as msg:
            raise Usage(msg)

        if len(args) != 2:
            raise Usage('python validation/Validation.py [OPTIONAL_PARAMS] [FILE_WITH_VALIDATION] [FILE_WITH_EXTRACT]\n\t[OPTIONAL_PARAMS]: -s to return only true/false')

        validation_file = args[0]
        extraction_file = args[1]

        with codecs.open(validation_file, "r", "utf-8") as myfile:
             validation_str = myfile.read().encode('utf-8')
        validation_json = json.loads(validation_str)
        v = Validation(validation_json)

        with codecs.open(extraction_file, "r", "utf-8") as myfile:
             extraction_str = myfile.read().encode('utf-8')
        page_json = json.loads(extraction_str)
        new_page_json = v.validate_page(page_json)

        if simple:
            result = Validation.get_validation_for_page(new_page_json)
            print(json.dumps(result, indent=2, separators=(',', ': ')))
        else:
            print(json.dumps(new_page_json, indent=2, separators=(',', ': ')))

    except Usage as err:
        print(err.msg, end='', file=sys.stderr)
        print("for help use --help", end='', file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

#Sample of extraction with validation metadata
#
#   "extraction": {
#     "page1": {
#       "-whataboutthistable0005": {
#         "begin_index": 87,
#         "end_index": 96,
#         "extract": "Firstname",
#         "rule_id": "aa75710a-334d-458e-bcb5-f4298c8ea99b"
#       },
#       "0007": {
#         "begin_index": 106,
#         "end_index": 114,
#         "extract": "Lastname",
#         "rule_id": "607d9120-c56d-4c34-966c-3d470037f2ad",
#         "valid": false,
#         "validation": [
#           {
#             "param": "param1",
#             "type": "RULE1",
#             "valid": true
#           },
#           {
#             "type": "RULE3",
#             "valid": false
#           }
#         ]
#       },
#       "0012": {
#         "begin_index": 201,
#         "end_index": 239,
#         "extract": "Bubba</td> <td>Yourd</td> <td>66</td>",
#         "rule_id": "db221681-c05c-41e5-a22c-f4070e9314ff"
#       },
#       "Age0011": {
#         "begin_index": 148,
#         "end_index": 186,
#         "extract": "Bill</td> <td>Wilson</td> <td>33</td>",
#         "rule_id": "07fdbe8d-217b-47ae-b8ba-c2e8d5b43980"
#       },
#       "_list0001": {
#         "begin_index": 50,
#         "end_index": 313,
#         "extract": " <table style=\"width:100%\"> <tr> <th>Firstname</th> <th>Lastname</th> <th>Age</th> </tr> <tr> <td>Bill</td> <td>Wilson</td> <td>33</td> </tr> <tr> <td>Bubba</td> <td>Yourd</td> <td>66</td> </tr> </table> ENDOFPAGE",
#         "rule_id": "c06c2aa4-6150-4968-9968-2cb37cb6de13",
#         "sequence": [
#           {
#             "begin_index": 87,
#             "end_index": 131,
#             "extract": "Firstname</th> <th>Lastname</th> <th>Age</th",
#             "sequence_number": 1
#           },
#           {
#             "begin_index": 148,
#             "end_index": 184,
#             "extract": "Bill</td> <td>Wilson</td> <td>33</td",
#             "sequence_number": 2
#           },
#           {
#             "begin_index": 201,
#             "end_index": 237,
#             "extract": "Bubba</td> <td>Yourd</td> <td>66</td",
#             "sequence_number": 3
#           }
#         ],
#         "valid": false,
#         "validation": [
#           {
#             "param": "param3",
#             "type": "RULE3",
#             "valid": false
#           }
#         ]
#       },
#       "us_state0001": {
#         "begin_index": 16,
#         "end_index": 20,
#         "extract": "me",
#         "rule_id": "180c5bf4-50ee-42f0-b0f1-6339d290f207"
#       }
#     }
#   }
# }}