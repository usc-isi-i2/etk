from setuptools import setup, find_packages

config = {
    'name': 'landmark_extractor',
    'description': 'A regular expression based extraction tool for extracting structured text from strings.',
    'author': 'InferLink',
    'url': 'https://github.com/inferlink/landmark-extractor',
    'download_url': 'https://github.com/inferlink/landmark-extractor',
    'author_email': 'developers@inferlink.com',
    'version': '0.1.8.0',
    'license': 'GNU AGPL',
    'packages': find_packages(),
    'classifiers': [],
    'install_requires': ['regex==2018.1.10']
}

setup(**config)
