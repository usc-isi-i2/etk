ETK: Information Extraction Toolkit
============================

.. begin-intro
.. image:: https://img.shields.io/badge/license-MIT-blue.svg
    :target: https://raw.githubusercontent.com/usc-isi-i2/etk/master/LICENSE
    :alt: License

.. image:: https://api.travis-ci.org/usc-isi-i2/etk.svg?branch=master
    :target: https://travis-ci.org/usc-isi-i2/etk
    :alt: Travis

.. image:: https://badge.fury.io/py/etk.svg
    :target: https://badge.fury.io/py/etk
    :alt: pypi

.. image:: https://readthedocs.org/projects/etk/badge/?version=latest
    :target: http://etk.readthedocs.io/en/latest
    :alt: Documents

ETK is a Python library for high precision information extraction from many document formats.
It proivdes a flexible framework of **composable extractors** that enables you to combine a host of **predefined extractors** provided in ETK with custom extractors that you may need to develop for your application.
It supports extraction from HTML pages, text documents, CSV and Excel files and JSON documents.
ETK is open-source software, released under the MIT license.

**Features**

* Extraction from HTML, text, CSV, Excel, JSON
* High-precision predefined extractors for common entities (dates, phones, email, cities, ...)
* Extraction of microdata, schema.org and RDFa markup
* Integration with [spaCy](https://github.com/explosion/spaCy) for text processing
* Automatic identification and extraction of HTML tables containing data
* Automatic identification and extraction of time series
* Semi-automatic generation of Web wrappers
* Scalable execution and management of extraction pipelines
* Automatic provenance recording

**Releases**

- [Source code](https://github.com/usc-isi-i2/etk/releases)
- [Docker images](https://hub.docker.com/r/uscisii2/etk/tags/)


.. end-intro
