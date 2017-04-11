# etk
![travis ci](https://travis-ci.org/usc-isi-i2/etk.svg?branch=development)  

## Setup
`conda-env create .`  
`source activate etk_env`   
`python -m spacy download en`  

## Run Tests  
`python -m unittest discover`

This repository will contain our toolkit for extracting information from web pages.
It will be built in stages to contain the following capabilities:

* Several structure extractors to identify the main content of a page and tables
* A host of data extractors for common entities, including people, places, phone, email, dates, etc.
* A trainable algorithm to rank extractions
* Automated experimentation to measure precision and recall of extractions
