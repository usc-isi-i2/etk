# -*- coding: utf-8 -*-
# @Author: ZwEin
# @Date:   2016-09-30 15:37:23
# @Last Modified by:   ZwEin
# @Last Modified time: 2016-11-16 16:28:51


import re
from sets import Set

################################################
# Content
################################################

DE_SOCIAL_MEDIA_NAMES = [
    "Facebook",
    "QQ",
    "WhatsApp",
    "QZone",
    "WeChat",
    "LinkedIn",
    "Skype",
    "Instagram",
    "Baidu ",
    "Tieba",
    "Twitter",
    "Viber",
    "Tumblr",
    "Snapchat",
    "LINE",
    "Sina",
    "Weibo",
    "VK",
    "Reddit",
    "YY",
    "Telegram",
    "Tagged",
    "Myspace",
    "Badoo",
    "StumbleUpon",
    "Foursquare",
    "MeetMe",
    "Meetup",
    "Delicious",
    "Snapfish",
    "DeviantArt",
    "Fotolog",
    "Buzznet",
    "We Heart It",
    "Path",
    "Flixster",
    "Gaia Online",
    "CaringBridge",
    "VampireFreaks",
    "CafeMom",
    "Ravelry",
    "ASmallWorld",
    "Nextdoor",
    "Wayn",
    "TravBuddy",
    "Cellufun",
    "MocoSpace",
    "YouTube",
    "Youku",
    "Tout",
    "Vine",
    "Classmates",
    "MyLife",
    "MyHeritage",
    "Ryze",
    "Xing",
    "WeeWorld",
    "Habbo",
    "Tuenti",
    "Solaborate",
    "Plurk",
    "LiveJournal",
    "Mixi",
    "Douban",
    "Renren",
    "Odnoklassniki",
    "NK",
    "Netlog",
    "StudiVZ",
    "Friendster",
    "Draugiem",
    "Glocals"
]

DE_SOCIAL_MEDIA_NAMES = [_.lower() for _ in DE_SOCIAL_MEDIA_NAMES]

################################################
# Main
################################################

class DIGEmailExtractor(object):
    """Extractor of email addresses from text.
    The legal definition is in https://en.wikipedia.org/wiki/Email_address

    This class attempts to map purposefully obfuscated email addresses to legal addresses.

    Users of this class should call DIGEmailExtractor.extract_email(), see documentation.
    The main program is to test against the ground truth.
    """

    def __init__(self):

        self.common_domains = [
            "gmail",
            "gee mail",
            "g mail",
            "gml",
            "yahoo",
            "hotmail"
        ]
        self.common_domains_regex = "(?:" + "|".join(self.common_domains) + ")"

        self.gmail_synonyms = [
            "gee mail",
            "g mail",
            "gml"
        ]
        self.gmail_synonyms_regex = r"(" + "|".join(self.gmail_synonyms) + ")"

        self.com_synonyms = [
            r"com\b",
            r"co\s*\.\s*\w\w\w?",
            r"co\s+dot\s+\w\w\w?"
        ]
        self.com_synonyms_regex = r"(?:" + "|".join(self.com_synonyms) + ")"

        # The intent here is to match things like "yahoo com", "yahoo dot com"
        # We require matching the com synonyms to avoid interpreting text that
        # contains "at yahoo" as part of a domain name.
        self.spelled_out_domain_regex = r"(?:" + self.common_domains_regex + \
            "(?:(?:dot\s+|\.+|\,+|\s+)" + self.com_synonyms_regex + "))"
        # print "spelled_out_domain_regex:%s" % spelled_out_domain_regex

        self.at_regexes = [
            r"@",
            r"\(+@\)+",
            r"\[+@\]+",
            r"\(+(?:at|arroba)\)+",
            r"\[+(?:at|arroba)\]+",
            r"\{+(?:at|arroba)\}+",
            r"\s+(?:at|arroba)@",
            r"@at\s+",
            r"at\s+(?=" + self.spelled_out_domain_regex + ")",
            r"(?<=\w\w\w|\wat)\s+(?=" + self.spelled_out_domain_regex + ")",
            r"(?<=\w\w\w|\wat)\[\](?=" +
            self.spelled_out_domain_regex + "?" + ")"
        ]
        self.at_regex = "(?:" + r'|'.join(self.at_regexes) + ")"

        # People put junk between the "at" sign and the start of the domain
        self.at_postfix_regexes = [
            ",+\s*",
            "\.+\s*"
        ]
        self.at_postfix_regex = "(?:" + \
            r'|'.join(self.at_postfix_regexes) + ")?"

        self.full_at_regex = self.at_regex + self.at_postfix_regex + "\s*"

        # Character set defined by the standard
        self.basic_dns_label_regex = r"[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9]"
        self.non_dns_regex = r"[^a-zA-Z0-9\-.]"

        # Sometimes people do things like maria at (yahoo) (dot) (com)
        self.wrapped_basic_dns_label_regexes = [
            self.basic_dns_label_regex,
            "\(+" + self.basic_dns_label_regex + "\)+",
            "\[+" + self.basic_dns_label_regex + "\]+"
        ]
        self.dns_label_regex = "(?:" + \
            "|".join(self.wrapped_basic_dns_label_regexes) + ")"

        # People put all kinds of junk between the parts of a domain name
        self.dot_regex = "[(\[]*dot[)\]]*"
        self.dns_separator_regexes = [
            "\s*\.+\s*",
            "[\.\s]+" + self.dot_regex + "[\.\s]+",
            "\(+(?:\.|" + self.dot_regex + ")+\)+",
            "\[+\.+\]+",
            "\{+\.+\}+",
            "\s+(?=" + self.com_synonyms_regex + ")"
        ]
        self.dns_separator_regex = "(?:" + ",*" + \
            "|".join(self.dns_separator_regexes) + ",*" + ")"

        self.dns_re = self.full_at_regex + \
            r"(" + self.dns_label_regex + r"(?:" + \
            self.dns_separator_regex + self.dns_label_regex + r")*)"

        #
        # Regex for the user name part of legal addresses.
        # Assuming all text has been lowercased before.
        #
        # Assuming that special characters are not used, this can be added later.
        # from wikipedia: space and "(),:;<>@[\] characters are allowed with restrictions
        # all allowed: !#$%&'*+-/=?^_`{|}~ and space
        # allowed without quoting: !#$%&'*+-/?^_`{|}~, dot can appear, but not
        # at the beginning

        # The full set requires starting with alphanumeric, this is because of all the junk
        # that appears often. Also require at least 4 characters.
        # full_username_regex = r"[a-z0-9][a-z0-9.!#$%&'*+-/?^_`{|}~]{3,}"
        self.full_username_regex = r"[a-z0-9]+(?:[-.!#$%&'*+/?^_`{|}~][a-z0-9]+)*"

        # The basic regex is for cases when there is no @ sign, which means there was plenty
        # of obfuscation and the potential for all kinds of decoration which we don't want in
        # the email address. We don't allow consecutive punctuation to avoid grabbing emails
        # such as me.......LouiseHolland41@gmail
        self.basic_username_regex = r"(?:[a-z0-9]+(?:(?:[-+_.]|[(]?dot[)]?)[a-z0-9]+)*\s*)"

        # use lookahead to find the @ immediately following the user name, with
        # possible spaces.
        self.strict_username_regex = r"(?:" + \
            self.full_username_regex + r"(?=@))"

        self.username_regex = r"(" + self.basic_username_regex + \
            r"|" + self.strict_username_regex + r")"

        self.email_regex = self.username_regex + self.dns_re

    ################################################
    # Clean
    ################################################
    
    def clean_domain(self, regex_match):
        """Once we compute the domain, santity check it, being conservative and throwing out
        suspicious domains. Prefer precision to recall.

        :param regex_match: the output of our regex matching
        :type regex_match: string
        :return:
        :rtype:
        """
        result = regex_match
        result = re.sub(self.gmail_synonyms_regex, "gmail", result)
        result = re.sub("\s+", ".", result)
        result = re.sub(self.dot_regex, ".", result)
        result = re.sub(self.non_dns_regex, "", result)
        result = re.sub("\.+", ".", result)
        result = result.strip()

        # If the domain ends with one of the common domains, add .com at the
        # end
        if re.match(self.common_domains_regex + "$", result):
            result += ".com"
        # All domains have to contain a .
        if result.find('.') < 0:
            return ''
        # If the doman contains gmail, it has has to be gmail.com
        # This is drastic because of examples such as "at faithlynn1959@gmail.
        # in call"
        if result.find('gmail') >= 0:
            if result != 'gmail.com':
                return ''
        return result

    @staticmethod
    def clean_username(string):
        """

        :param string:
        :type string:
        :return:
        :rtype:
        """
        username = string.strip()
        username = re.sub("[(]?dot[)]?", '.', username)
        # paranoid sanity check to reject short user names.
        if len(username) < 4:
            return None
        return username

    def clean_match(self, m):
        u = m.group(1)
        d = m.group(2)
        domain = self.clean_domain(d)
        username = DIGEmailExtractor.clean_username(u)
        if domain and username:
            email = username + "@" + domain
            return email
        return None

    ################################################
    # Extract
    ################################################

    def extract_domain(self, string):
        """Extract the domain part of an email address within a string.
        Separate method used for testing purposes only.
        :param string:
        :return:
        :rtype:
        """
        matches = re.findall(self.dns_re, string, re.I)

        clean_results = []
        for m in matches:
            clean_results.append(self.clean_domain(m))
        return clean_results

    def is_email_match_obfuscated(self, clean_email, unclean_email_match):
        unclean_email = unclean_email_match.group(1).strip() + "@" + unclean_email_match.group(2).strip()
        return clean_email != unclean_email

    @staticmethod
    def prepare_input_string(string):
        line = string.lower().replace('\n', ' ').replace('\r', '')
        line = re.sub(r"[*?]+", " ", line)
        line = re.sub(r"\\n", " ", line)
        line = re.sub(r"\s+g\s+mail\s+", " gmail ", line)
        return line

    def extract_usernames_and_domains_matches(self, string):
        line = DIGEmailExtractor.prepare_input_string(string)
        return re.finditer(self.email_regex, line)

    def extract_email(self, string):
        """Extract email address from string.
        :param string: the text to extract from
        :param return_as_string: whether to return the result as a string of comma-separated values or
        as a set
        :type return_as_string: Boolean
        """

        clean_results = list()

        for m in self.extract_usernames_and_domains_matches(string):
            clean_email = self.clean_match(m)
            clean_results.append(clean_email)

        return list(frozenset(clean_results))

    def extract_email_with_context(self, string):
        # TODO refactor this so we aren't creating context manually
        clean_results = list()
        for m in self.extract_usernames_and_domains_matches(string):
            clean_email = self.clean_match(m)
            if not clean_email:
                continue

            username, domain = clean_email.split('@')

            if username in DE_SOCIAL_MEDIA_NAMES:
                continue

            context = {}
            context['value'] = clean_email
            context['field'] = 'text'
            context['start'] = m.start()
            context['end'] = m.end()
            context['obfuscation'] = self.is_email_match_obfuscated(clean_email, m)
            clean_results.append(context)
        return clean_results

    


