from etk.extractor import Extractor, InputType
from etk.extraction import Extraction
from typing import List
from email.utils import parsedate_to_datetime
from bs4 import BeautifulSoup
import re
import json
import datetime

class MailmanExtractor(Extractor):
    def __init__(self, email_url: str, mailing_list_name: str, extractor_name: str) -> None:

        """
        Initialize the extractor, storing mailing list and message information
        Args:
            email_url: str
            mailing_list_name: str
            extractor_name: str

        Returns:
        """
        
        Extractor.__init__(self,
                           input_type=InputType.TEXT,
                           category="build_in_extractor",
                           name=extractor_name)

        self.email_url = email_url
        self.mailing_list_name = mailing_list_name

    def old_format(self, content: BeautifulSoup) -> List[str]:

        """
        Extracts email message information if it uses the old Mailman format
        Args:
            content: BeautifulSoup

        Returns: List[str]
        """
        
        b = content.find('body')
        sender, date, nxt, rep_to = None, None, None, None
        strongs = b.findAll('strong', recursive=False)
        for s in strongs:
            field = str(s).split(">")[1].split("<")[0]
            if 'From' in field:
                sender = s.next_sibling.split("(")[0].strip()
            elif 'Date' in field:
                date_str = s.next_sibling.strip().replace("-","").replace("  "," ").strip()
                try:
                    date = parsedate_to_datetime(date_str).isoformat()[:19]
                except:
                    date = None
        sender = b.find('b').text if sender == None else sender
        sender = b.find('a').text if len(sender) == 0 else sender
        date = b.find('i').text[:19] if date == None else date

        try:
            nav = content.find('ul').findAll('li')
        except:
            nav = None
        if nav != None:
            for l in nav:
                s = l.text
                if 'Next in thread' in s:
                    nxt = '/'.join(self.email_url.split('/')[:-1]) + '/' + l.find('a')['href']
                    nxt = nxt[1:] if nxt[0] == '/' else nxt
                elif 'reply to' in s:
                    rep_to = '/'.join(self.email_url.split('/')[:-1]) + '/' + l.find('a')['href']
                    rep_to = rep_to[1:] if rep_to[0] == '/' else rep_to
        body = content.find('pre')
        body = body.text.strip() if body != None else None
        return [str(i) for i in [sender, date, body, nxt, rep_to]]

    def new_format(self, navbar: BeautifulSoup, content: BeautifulSoup) -> List[str]:

        """
        Extracts email message information if it uses the new Mailman format
        Args:
            content: BeautifulSoup

        Returns: List[str]
        """
        
        sender = content.find(id='from').text.split('via')[0][6:].strip()
        date_str = content.find(id='date').text.split(': ')[1].strip()
        date = parsedate_to_datetime(date_str).isoformat()[:19]
        body = content.find(id='body').text.strip()
        nxt, rep_to = None, None
        
        links = navbar.findAll('a')
        for l in links:
            if 'Next in thread' in str(l):
                nxt = '/'.join(self.email_url.split('/')[:-1]) + '/' + l['href']
                nxt = nxt[1:] if nxt[0] == '/' else nxt
            elif 'reply to' in str(l):
                rep_to = '/'.join(self.email_url.split('/')[:-1]) + '/' + l['href']
                rep_to = rep_to[1:] if rep_to[0] == '/' else rep_to
        return [str(i) for i in [sender, date, body, nxt, rep_to]]
    
    def extract(self, text: str) -> List[Extraction]:

        """
        Extracts and structures email message from UTF8-encoded text
        Args:
            text: str

        Returns: Extraction
        """
        
        content = BeautifulSoup(text, 'html5lib')
        subject = content.find('h1').text.strip()
        recip = self.mailing_list_name
        
        navbar = content.find(id='navbar')
        if navbar == None:
            info = self.old_format(content)
        else:
            info = self.new_format(navbar, content)
        for i in info[0:3]:
            if i == 'None':
                print('missed something important')
        sender = info[0]
        date   = info[1]
        body   = info[2]
        nxt    = info[3]
        rep_to = info[4]
        pub = 'SeeSat_Obs'
        dRec = datetime.datetime.now().isoformat()
        
        msg_obj = { 
            'url' : self.email_url,
            '@context' : {
                '@vocab' : 'schema.org'
            },
            'subject' : subject,
            'recip' : recip,
            'sender' : sender
        }
        if date != 'None':
            msg_obj['dateReceived'] = date
        if body != 'None':
            msg_obj['body'] = body
        if nxt != 'None':
            msg_obj['nxt'] = nxt
        if rep_to != 'None':
            msg_obj['replyToMessage'] = rep_to
        return Extraction(value=msg_obj, extractor_name=self.name)
