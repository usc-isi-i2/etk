# etk2
![travis ci](https://travis-ci.org/usc-isi-i2/etk.svg?branch=etk2)
This repository will contain our new version toolkit for extracting information from web pages based on python3
It will be built in stages to contain the following capabilities:

* Several structure extractors to identify the main content of a page and tables
* A host of data extractors for common entities, including people, places, phone, email, dates, etc.
* A trainable algorithm to rank extractions
* Automated experimentation to measure precision and recall of extractions  
## Setup
`conda-env create .`  
`source activate etk2_env`
`python -m spacy download en_core_web_sm`

## Run Tests
`cd etk2`
`python -m unittest discover`
