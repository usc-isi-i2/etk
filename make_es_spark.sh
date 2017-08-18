#!/usr/bin/env bash
rm -rf es_lib
mkdir es_lib
cd es_lib

cp -r ~/Downloads/elasticsearch-2.4.0/elasticsearch .
cp -r ~/Downloads/urllib3-1.22/urllib3 .
zip -r python-lib.zip *
cd ..
