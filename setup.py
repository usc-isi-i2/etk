import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="etk",
    version="2.0.1",
    author="Amandeep Singh",
    author_email="amandeep.s.saggu@gmail.com",
    description="extraction toolkit",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/usc-isi-i2/etk",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ),
)
