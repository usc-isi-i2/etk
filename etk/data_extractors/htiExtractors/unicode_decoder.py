
### @author Rishi Jatia


import json
import re
import string


def decode_unicode(data, replace_boo=True):
  # dictionary which direct maps unicode values to its letters
  dictionary = {'0030':'0','0031':'1','0032':'2','0033':'3','0034':'4','0035':'5','0036':'6','0037':'7','0038':'8','0039':'9','0024':'$','0040':'@','00A2':'cents','00A3':'pounds','00A5':'yen','00C7':'C','00D0':'D','00D1':'N','00DD':'Y','00E7':'c','00F1':'n','00FD':'y','00FF':'y','010E':'D','010F':'F','0110':'D','0111':'D','0130':'I','0134':'J','0135':'J','0136':'K','0137':'K','0138':'K','0160':'S','0161':'S','0191':'F','0192':'F','0193':'G','0198':'K','0199':'K',
  '019D':'N','019E':'N','01A4':'P','01A5':'P','01AC':'T','01AF':'U','01B5':'Z','01CD':'A','01CE':'A','01CF':'I','01D0':'I','01D1':'O','01D2':'O','01DE':'A','01DF':'A','01E0':'A','01E1':'A','01F4':'G','01F5':'G','01F8':'N','01F9':'N','01FA':'A','01FB':'A','021E':'H',
  '021F':'H','0224':'Z','2113':'L','2718':'X','0225':'Z','2134':'O','0226':'A','0227':'A','0228':'E','0229':'E','0386':'A','0388':'E','0389':'H','038A':'I','0391':'A','0392':'B','0395':'E','0396':'Z','0397':'H','0399':'I','039A':'K','039C':'M','039D':'N','039F':'O','03A1':'P','03A4':'T','03A5':'Y','03A7':'X','03AA':'I','03AB':'B','1E10':'D','1E11':'D','1E12':'D','1E13':'D','1E1E':'F','1E1F':'F','1E20':'G','1E21':'H','1E2C':'I','1E2D':'I','1E2E':'I','1E2F':'I','1E3E':'M','1E3F':'M','1E70':'T','1E71':'T','1E8E':'Y','1E8F':'Y','1EE0':'O','1EE1':'O','1EE2':'O','1EE3':'O','1EE4':'O','1EF0':'U','1EF1':'U'}
  # dictionary in which patterns (prefixes and suffixes) are matched to possible letter choices
  pattern_dict = {'00C':'AEI', '00D':'OU','00E':'AEI','00F':'OU','010':'AC','011':'EG','012':'GHI','013':'L','014':'LNO','015':'RS','016':'TU','017':'UWYZ', '018':'BCD','01D':'U','01E':'GKO','020':'AEIO','021':'RUST','022':'O','1E0':'ABCD','1E1':'E','1E3':'KL','1E4':'MNO','1E5':'OPR','1E6':'ST','1E7':'UV','1E8':'WX','1E9':'Z','1EB':'A','1EC':'EIO','1ED':'O','1EE':'U','1EF':'Y','216':'greeknum','217':'greeknum','246':'consecnum','247':'numfrom17'}
  #dictionary which matches patterns for emoticons
  hex_dict = {'A':'10','B':'11','C':'12','D':'13','E':'14','F':'15','a':'10','b':'11','c':'12','d':'13','e':'14','f':'15'}
  happy_dict = ['1F600','263A','1F601','1F602','1F603','1F604','1F605','1F606','1F60A','263A','1F642','1F607','1F60C','1F643','1F62C','1F63A','1F638','1F639']
  sad_dict = ['1F610','1F611','1F623','1F494','1F625','1F62B','1F613','1F614','1F615','2639','1F641','1F616','1F61E','1F61F','1F624','1F622','1F62D','1F629','1F630','1F620']
  sexual_dict = ['1F609','1F6C0','2B50','1F445','1F525','1F36D','2606','1F60D','1F460','1F618','1F617','1F61A','1F917','1F60F','1F63B','1F63D','1F483','1F46F','1F48F','1F444','1F48B','1F459','1F484','1F34C','1F4AF','264B']
  hearts=['1F498','2664','2764','2661','2665','1F493','1F495','1F496','1F497','1F499','1F49A','1F49B','1F49C','1F49D','1F49E','1F49F','2763']
  baseball_dict=['26BE', '1F3C0', '1F3CF']
  count=0

  misc_code = ' *misc* '
  if not replace_boo:
    misc_code = ''

  retval=''
  # first I am filtering out all the non-unicode characters from the data 
  regex=re.compile(r'\\u[0-9ABCDEFabcdef]{1,4}')
  regex2=re.compile(r'\\U[0-9ABCDEFabcdef]{1,8}') #this is so that both types of unicode representations are filtered
  lowers = list('abcdef')
  uppers = [c.upper() for c in lowers]
  ndata = set()
  data = data.encode('unicode-escape').decode('utf-8')
  data = re.sub(r'(?:\\x(?:[0-9]|[a-f]){2})+', ' ', data, flags=re.IGNORECASE)
  for val in re.finditer(regex,data):
    to_append=val.group()
    #converting unicode to standard representation
    for c in lowers:
      if c in to_append:
        to_append = to_append.replace(c, c.lower())
    ndata.add(to_append)
  for val in re.finditer(regex2,data):
    to_append = '\u' + val.group()[5:]
    for c in lowers:
      if c in to_append:
        to_append = to_append.replace(c, c.lower())
    ndata.add(to_append)
  ndata = list(ndata)
  """
  Process of parsing:
  -> Convert unicode into standard form
  -> Convert each character of the unicode symbol to its numerical equivalent
  -> Mapping Process:
    -  First check in pattern dictionary to map suffix/prefix
    -  Check Emoticon Dictionary 
    -  Replace value pair with Key whenever found
    -  Then check direct dictionary
    -  Append to .txt file if unicode not found in any dictionary 
  """
  for unicode_str in ndata:
    uni=unicode_str[2:]
    if unicode_str not in data:
      unicode_str='\U000' + unicode_str[2:]
      #converting to standard representation
      for c in uppers:
        if c in unicode_str:
          unicode_str = unicode_str.replace(c, c.lower())
    if uni in baseball_dict:
      retval+=' *baseball* '
      #detecting baseball emoticons and converting to '*baseball*' and similar conversions for other categories of emoticons
      data=string.replace(data,unicode_str,' *baseball* ')
    if uni in happy_dict:
      retval+=' *happy* '
      if replace_boo:
        data=string.replace(data,unicode_str,' *happy* ')
      else:
        data=string.replace(data,unicode_str,' ')
    elif uni in sad_dict:
      retval+=' *sad* '
      if replace_boo:
        data=string.replace(data,unicode_str,' *sad* ')
      else:
        data=string.replace(data,unicode_str,' ')
    elif uni in sexual_dict:
      retval+=' *sexual* '
      if replace_boo:
        data=string.replace(data,unicode_str,' *sexual* ')
      else:
        data=string.replace(data,unicode_str,' ')
    elif uni in hearts:
      retval+=' *hearts* '
      if replace_boo:
        data=string.replace(data,unicode_str,' *hearts* ')
      else:
        data=string.replace(data,unicode_str,' ')
    elif uni in dictionary:
      retval+=dictionary[uni]
      data=string.replace(data,unicode_str,dictionary[uni])
    elif uni[0:3]=='004' or uni[0:3]=='005':
      #replacing unicodes for digits and before that, replacing hexadecimals with their numerical value
      last_dig=uni[3:]
      if last_dig in hex_dict:
        last_dig=int(hex_dict[last_dig])
      else:
        last_dig=int(last_dig)
      second_last_dig= int(uni[2:3])
      num= (second_last_dig-4)*16 + last_dig
      retval+=chr(64+num)
      data=string.replace(data,unicode_str,chr(64+num))
    elif uni[0:3]=='006' or uni[0:3]=='007':
      last_dig=uni[3:]
      if last_dig in hex_dict:
        last_dig=int(hex_dict[last_dig])
      else:
        last_dig=int(last_dig)
      second_last_dig= int(uni[2:3])
      #parsing letters
      num= (second_last_dig-6)*16 + last_dig
      retval+=chr(64+num)
      data=string.replace(data,unicode_str,chr(64+num))
    elif uni[0:3] in pattern_dict:
      val = pattern_dict[uni[0:3]]
      if len(val)==1:
        retval+=val
        data=string.replace(data,unicode_str,val)
      elif uni[0:3]=='00C':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          #parsing miscelleneous 
          data=string.replace(data,unicode_str,misc_code)
        if last>=0 and last<=5:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last<=11:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        else:
          retval+=val[2]
          data=string.replace(data,unicode_str,val[2])
      elif uni[0:3]=='00D':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          pass
        if last>=2 and last<=6:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last>=9 and last<=12:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='00E':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=0 and last<=5:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last<=11:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        else:
          retval+=val[2]
          data=string.replace(data,unicode_str,val[2])
      elif uni[0:3]=='00F':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=2 and last<=6:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last>=9 and last<=12:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='010':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=0 and last<=5:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last>=6 and last<=13:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='011':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=2 and last<=11:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last>=12 and last<=15:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='012':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=0 and last<=3:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last<=7:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        elif last<=15:
          retval+=val[2]
          data=string.replace(data,unicode_str,val[2])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='014':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=0 and last<=2:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last<=11:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        elif last<=15:
          retval+=val[2]
          data=string.replace(data,unicode_str,val[2])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='015':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=4 and last<=9:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last>=10 and last<=15:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='016':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=2 and last<=7:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last>=8 and last<=15:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='017':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=0 and last<=3:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last<=5:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        elif last<=8:
          retval+=val[2]
          data=string.replace(data,unicode_str,val[2])
        elif last<=14:
          retval+=val[3]
          data=string.replace(data,unicode_str,val[3])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='018':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=0 and last<=5:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last<=8:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        elif last<=12:
          retval+=val[2]
          data=string.replace(data,unicode_str,val[2])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='01E':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=4 and last<=7:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last<=9:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        elif last<=13:
          retval+=val[2]
          data=string.replace(data,unicode_str,val[2])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='020':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=0 and last<=3:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last<=7:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        elif last<=11:
          retval+=val[2]
          data=string.replace(data,unicode_str,val[2])
        elif last<=15:
          retval+=val[3]
          data=string.replace(data,unicode_str,val[3])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='021':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=0 and last<=3:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last<=7:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        elif last<=9:
          retval+=val[2]
          data=string.replace(data,unicode_str,val[2])
        elif last<=11:
          retval+=val[3]
          data=string.replace(data,unicode_str,val[3])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='1E0':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=0 and last<=1:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last<=7:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        elif last<=9:
          retval+=val[2]
          data=string.replace(data,unicode_str,val[2])
        elif last<=15:
          retval+=val[3]
          data=string.replace(data,unicode_str,val[3])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='1E3':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=0 and last<=5:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last<=13:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='1E4':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          pass
        if last>=0 and last<=3:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last<=11:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        elif last<=15:
          retval+=val[2]
          data=string.replace(data,unicode_str,val[2])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='1E5':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          pass
        if last>=0 and last<=3:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last<=7:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        elif last<=15:
          retval+=val[2]
          data=string.replace(data,unicode_str,val[2])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='1E6':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=0 and last<=9:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last>=10 and last<=15:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='1E7':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=2 and last<=11:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last>=12 and last<=15:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='1E8':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=0 and last<=9:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last>=10 and last<=13:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='1EC':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last>=0 and last<=7:
          retval+=val[0]
          data=string.replace(data,unicode_str,val[0])
        elif last<=11:
          retval+=val[1]
          data=string.replace(data,unicode_str,val[1])
        elif last<=15:
          retval+=val[2]
          data=string.replace(data,unicode_str,val[2])
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='216' or uni[0:3]=='217':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if last<=12:
          retval+=str(last+1)
          data=string.replace(data,unicode_str,str(last+1))
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
      elif uni[0:3]=='246' or uni[0:3]=='247':
        last=uni[3:]
        if last in hex_dict:
          last=hex_dict[last]
        try:
          last=int(last)
        except:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
        if uni[0:3]=='246':
          retval+=str(last+1)
          data=string.replace(data,unicode_str,str(last+1))
        elif last<=3:
          retval+=str(last+17)
          data=string.replace(data,unicode_str,str(last+17))
        else:
          retval+=misc_code
          data=string.replace(data,unicode_str,misc_code)
    else:
      retval+=misc_code
      data = data.replace(unicode_str,misc_code)

  if len(retval)==0:
    retval="Sorry, no unicode strings were present"
  try:
    data = data.decode('unicode-escape')
  except UnicodeDecodeError:
    pass
  retval = retval.encode('unicode-escape').decode('unicode-escape')
  return (retval, data)
