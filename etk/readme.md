# Extraction Toolkit

An API for all extractors.

* Output for each extractor will be in this format -
```
[{
  "value": "",
  "context": {}
}]
```
* ```core.py ``` will hold all the main methods for the api and call the extractors. ```init() ``` method loads all the extractors and the other methods just call them with the given params.
* Each extractor will have its own file. Data extractors go in ```data-extractors```.
* Each extractor will have its own file for unit testing in the ```tests``` folder. We will be using pytest for this.
* Names for the methods to be decided. See examples for sample usage.
* Docs using sphinx.
