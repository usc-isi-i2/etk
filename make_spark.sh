#!/usr/bin/env bash
rm -rf lib
mkdir lib
cd lib

mkdir etk

cp ../etk/*.py etk/
cp -r ../etk/data_extractors etk/
cp -r ../etk/spacy_extractors etk/
cp -r ../etk/structured_extractors etk/
zip -r python-lib.zip *
cd ..
