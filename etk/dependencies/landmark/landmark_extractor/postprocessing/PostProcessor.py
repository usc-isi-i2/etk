import abc
import re

def removeExtraSpaces(input_string):
    processor = RemoveExtraSpaces(input_string)
    return processor.post_process()

class Processor(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def post_process(self):
        pass
    
    def __init__(self, input_string):
        self.input_string = input_string

class RemoveExtraSpaces(Processor):
    def post_process(self):
        nbsp = re.sub('&nbsp;', ' ', self.input_string)
        return re.sub("\\s+", " ", nbsp).strip()
        
    def __init__(self, input_string):
        Processor.__init__(self, input_string)

class RemoveHtml(Processor):
# Code from Fetch
#     String startAndEndOfTag = "<[^ \t][^>]*[^ \t]>";
#     String singleCharacterTag = "<[^ \t>]>";
#     String startOfTagOnly = "<([^ \t][^>]*)?$";
#     String endOfTagOnly = "^([^<]*[^ \t])?>";
#     String patternString = singleCharacterTag + "|" + startAndEndOfTag + "|" + startOfTagOnly + "|" + endOfTagOnly; 
#     try{
#       s_stripHtmlPattern = (new Perl5Compiler()).compile(patternString, Perl5Compiler.READ_ONLY_MASK | Perl5Compiler.SINGLELINE_MASK);
#     }
#
#   public static String stripHtml(String s){
#     Substitution substitution = RegExps.makeSubstitution("");
#     String text = RegExps.substituteAllMatches(s,s_stripHtmlPattern,substitution);
#     return text;
#   }
    
    def post_process(self):
        return re.sub(self.patternString, "", self.input_string)
        
    def __init__(self, input_string):
        #first remove the <br> and <br/> from the input_strgin
        cleaned_input = re.sub(r'<br\s*/?>', ' ', input_string)
        cleaned_input = re.sub(r'<BR\s*/?>', ' ', cleaned_input)
        cleaned_input = re.sub(r'<bR\s*/?>', ' ', cleaned_input)
        cleaned_input = re.sub(r'<Br\s*/?>', ' ', cleaned_input)
        cleaned_input = re.sub(r'<a', ' <a', cleaned_input)
        Processor.__init__(self, cleaned_input)
        startAndEndOfTag = "<[^ \t][^>]*[^ \t]>"
        singleCharacterTag = "<[^ \t>]>"
        startOfTagOnly = "<([^ \t][^>]*)?$"
        endOfTagOnly = "^([^<]*[^ \t])?>"
        self.patternString = singleCharacterTag + "|" + startAndEndOfTag + "|" + startOfTagOnly + "|" + endOfTagOnly

if __name__ == '__main__':
    pass