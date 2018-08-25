Installation
============

.. note::

    ETK only supports Python 3 and it's tested under Python 3.6.

pip
----

Using pip to install::

    pip install etk

If you want to update ETK::

    pip install -U etk

Generally, it's recommended to install packages in a virtual environment::

    python3 -m venv etk2_env
    source etk2_env/bin/activate
    pip install etk

Load the spacy modules::

    python -m spacy download en_core_web_sm
    python -m spacy download en_core_web_lg

Install from source
-------------------

The other way to install ETK is to clone from GitHub repository and build it from source::

    git clone https://github.com/usc-isi-i2/etk.git
    pip install -e .

Install ETK from source and install packages in a virtual environment::

    git clone https://github.com/usc-isi-i2/etk.git
    cd etk
    python3 -m venv etk2_env
    source activate etk2_env
    pip install -e .

Load the spacy modules::

    python -m spacy download en_core_web_sm
    python -m spacy download en_core_web_lg

Run tests
---------
Run all ETK unit test::

    python -m unittest discover


Build documentation
-------------------

Documentation is powered by `Sphinx <http://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html>`_ , to generate it on your local, please run::

    cd docs
    make html # the generated doc is located at _build/html/index.html
