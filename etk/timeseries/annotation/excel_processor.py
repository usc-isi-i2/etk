import json
import logging
import argparse

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)


def process(filename):

    out_file = open("output.txt", mode='w')
    step = 1
    final_json = []

    for sheet, sheet_name, merged_cells in input_processor.process_excel(filename):
        logging.info("Processing sheet: %s", sheet_name)

        ps = sheet_processor.parsed_sheet(sheet_name, step, sheet, merged_cells)
        step += 1
        ps.find_tables(out_file)
        tmp = ps.get_output()
        if tmp == None:
            continue
        final_json.append(tmp)
        logging.debug(json.dumps(ps.get_output(), sort_keys=True, indent=2, separators=(',', ': ')))

    return final_json


def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("infile", help='File to annotate.')
    ap.add_argument("--outfile", help='File to write the annotation to.', default='')
    args = ap.parse_args()

    infile = args.infile.replace("\\", "")
    outfile = args.outfile.replace("\\", "")  # Handle case when outfile is empty

    if outfile == "":
        outfile = infile + "_annotation.json"

    final_json = process(infile)

    with open(outfile, mode='w') as out:
        json.dump(final_json, out, sort_keys=True, indent=2, separators=(',', ': '))
        logging.info("Output written to " + outfile)


if __name__ == "__main__":
    main()
