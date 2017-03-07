# This is how we intend to use it 
import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
import time
import etk

tk = etk.init()

start_time = time.time()

# Load all the dictionaries here
tk.load_dictionaries()

end_time = time.time()

print "Time taken to load all the tries: {0}".format(end_time - start_time)

print "\nCity Dictionary Extractor"
print tk.extract_using_dictionary(['portland'], name='cities')

print "\nCity Dictionary Extractor with structured tokens"
print tk.extract_using_dictionary([{"type": "normal", "value": "Los Angeles"}], name='cities', ngrams = 2)

print "\nHair Color Dictionary Extractor"
print tk.extract_using_dictionary(['brunette'], name='haircolor')

print "\nEthicities Dictionary Extractor"
print tk.extract_using_dictionary(['caucasian'], name='ethnicities')

print "\nEye Color Dictionary Extractor"
print tk.extract_using_dictionary(['brown'], name='eyecolor')

print "\nNames Dictionary Extractor"
print tk.extract_using_dictionary(['june'], name='names')

print "\nAddress Extractor"
print tk.extract_address('The LA area has many airports.  LAX is located at 1 World Way, Los Angeles, CA 90045.  The Bob Hope airport is at 2627 N Hollywood Way, Burbank, CA 91505.  Both are very busy.')

html_doc = """
<table><tr><td><a href=\"/otra.php?c=15&amp;article=overviewthumbs\" rel=\"nofollow\"><img height=\"110\" src=\"http://images.eroticmugshots.com/adthumbs/111.jpg\" width=\"110\"/><br/>BobabyGoJugg</a> 24<br/>Buffalo</td></tr><tr><td><a href=\"/otra.php?c=15&amp;article=overviewthumbs\" rel=\"nofollow\"><img height=\"110\" src=\"http://images.eroticmugshots.com/adthumbs/113.jpg\" width=\"110\"/><br/>HrumpMeNow</a> 24<br/>Buffalo</td></tr><tr><td><a href=\"/otra.php?c=15&amp;article=overviewthumbs\" rel=\"nofollow\"><img height=\"110\" src=\"http://images.eroticmugshots.com/adthumbs/108.jpg\" width=\"110\"/><br/>littlefrom</a> 25<br/>Buffalo</td></tr><tr><td><a href=\"/otra.php?c=15&amp;article=overviewthumbs\" rel=\"nofollow\"><img height=\"110\" src=\"http://images.eroticmugshots.com/adthumbs/76.jpg\" width=\"110\"/><br/>Ginaflirt</a> 28<br/>Buffalo</td></tr><tr><td><a href=\"/otra.php?c=15&amp;article=overviewthumbs\" rel=\"nofollow\"><img height=\"110\" src=\"http://images.eroticmugshots.com/adthumbs/56.jpg\" width=\"110\"/><br/>letsGETdwn</a> 24<br/>Buffalo</td></tr></table>", "fingerprint": "-108-110-111-113-15-24-25-28-56-76-BobabyGoJugg-Buffalo-Ginaflirt-HrumpMeNow-a-adthumbs-amp-article-br-c-com-eroticmugshots-height-href-http-images-img-jpg-letsGETdwn-littlefrom-nofollow-otra-overviewthumbs-php-rel-src-table-td-tr-width
"""
print "\nTable Extractor"
print tk.extract_table(html_doc)

doc = "32years old ,im 23"
print "\nAge Extractor"
print tk.extract_age(doc)

text = "Hair Long Blonde Languages Afrikaans English Body Type slender Age 20-24 Breasts A Eyes " \
	   "blue Height 1.78 Skin Fair Weight 51 Zandalee | Height 5'3\" Weight 103 | Invalid Height 220 Invalid " \
	   "Weight 10kg"
print "\nWeight Extractor"
print tk.extract_weight(text)

text = "Hair Long Blonde Languages Afrikaans English Body Type slender Age 20-24 Breasts A Eyes " \
       "blue Height 1.78 Skin Fair Weight 51 Zandalee | Height 5'3\" Weight 103 | Invalid Height 220 Invalid " \
       "Weight 10kg"
print "\nHeight Extractor"
print tk.extract_height(text)
