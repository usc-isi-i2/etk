# ETK: Information Extraction Toolkit

ETK is a Python library for high precision information extraction from many document formats.
It proivdes a flexible framework of **composable extractors** that enables you to combine a host of **predefined extractors** provided in ETK with custom extractors that you may need to develop for your application.
It supports extraction from HTML pages, text documents, CSV and Excel files and JSON documents.
ETK is open-source software, released under the MIT license.



![MIT License](https://img.shields.io/badge/license-MIT-blue.svg) ![travis ci](https://travis-ci.org/usc-isi-i2/etk.svg?branch=master)

## Documentation
Read the documentation [here](https://usc-isi-i2.github.io/etk/)

## Features

* Extraction from HTML, text, CSV, Excel, JSON
* High-precision predefined extractors for common entities (dates, phones, email, cities, ...)
* Extraction of microdata, schema.org and RDFa markup
* Integration with [spaCy](https://github.com/explosion/spaCy) for text processing
* Automatic identification and extraction of HTML tables containing data
* Automatic identification and extraction of time series
* Semi-automatic generation of Web wrappers
* Scalable execution and management of extraction pipelines
* Automatic provenance recording

# Releases

- [Source code](https://github.com/usc-isi-i2/etk/releases)
- [Docker images](https://hub.docker.com/r/uscisii2/etk/tags/)

## Installation

<table>
  <tr><td><b>Operating system:</td><td>macOS / OS X, Linux, Windows</td></tr>
  <tr><td><b>Python version:</td><td>Python 3.6+</td></tr>
<table>

Install using pip

```
pip install etk
```

### OR

You can also install ETK Manually. Clone or fork this repository, open a terminal window and in the directory where you downloaded ETK type the following commands

```
python3 -m venv etk2_env
source etk2_env/bin/activate
pip install -e .
```

Load the spacy modules
```
python -m spacy download en_core_web_sm
python -m spacy download en_core_web_lg (optional)
```
Note: If the above commands fail with s SSL error, run this:
```
python -m spacy download en_core_web_sm-2.0.0 --direct
```
To deactivate this virtual environment
```
deactivate
```

## Run Tests

`python -m unittest discover`

## Run ETK CLI

> ETK needs to be installed as python package.

`python -m etk <command> [options]`

For example:

`python -m etk regex_extractor "a.*c" "abcd"`

## Docker

Build image

`docker build -t etk:test .`

Run container

`docker run -it etk:dev /bin/bash`

Mount local volume for test

`docker run -it -v $(pwd):/app/etk etk:dev /bin/bash`

