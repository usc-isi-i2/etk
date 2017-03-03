import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
import etk

tk = etk.init()

# Age Extractor
doc = "32years old ,im 23"
print "\nAge Extractor"
print tk.extract_age(doc)
# API methods to be decided