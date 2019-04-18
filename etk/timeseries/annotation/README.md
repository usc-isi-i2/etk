Run `python excel_processor.py` and follow instructions.

Annotation file will be created as filename_annotation.json in the same directory as the file.

Object Hierarchy:

excel : sheet | sheets
sheet : table | tables
table : date_block, label_block, data_block
block : cell | cells

cells have classifier tags (date, text, number, empty, blank)