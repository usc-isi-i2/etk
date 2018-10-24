.. etk documentation master file, created by
   sphinx-quickstart on Wed Aug 22 17:09:21 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ETK: Information Extraction Toolkit
====================================

.. include:: ./../README.rst
      :start-after: begin-intro
      :end-before: end-intro


Getting Started
---------------

Installation (need to upload to PyPI later)::

   pip install -U etk

Example::

   >>> import etk
   >>>

Run ETK CLI::

   pip install -U etk
   python -m etk <command> [options]

For example::

   python -m etk dummy --test "this is a test"


Tutorial
-------------

.. toctree::
   :maxdepth: 2

   installation.rst
..   overview.rst

API Reference
-------------

.. toctree::
   :maxdepth: 3

   modules.rst
