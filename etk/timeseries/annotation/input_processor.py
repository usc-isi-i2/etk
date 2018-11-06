import pyexcel as pe
import xlrd
import logging


def process_excel(filename):
    if filename.endswith(".csv"):
        # add merged cells here please
        sheet = pe.get_sheet(file_name=filename)
        yield sheet, "name", ()
    else:
        book = pe.get_book(file_name=filename)
        sheets = book.to_dict()
        booklrd = xlrd.open_workbook(filename)
        for name in sheets.keys():
            sheet = book[name]
            fill_merged_cells(sheet, booklrd.sheet_by_name(name).merged_cells)
            yield sheet, name, booklrd.sheet_by_name(name).merged_cells


# filling the empty cells which are located in the merged cells in reality but are read as empty by pyexcel
def fill_merged_cells(sheet, merged_blocks):

    for block in merged_blocks:
        val = sheet[block[0], block[2]]
        for row in range(block[0], block[1]):
            for col in range(block[2], block[3]):
                try:
                    sheet[row, col] = val
                except:
                    # There is a difference in the way xlrd and pyexcel handle merged cells 
                    # Sometimes we end up accessing out of range cells
                    logging.error("Tried to access out of range cell.")
                    break
