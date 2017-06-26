# Readme

Clone the repo from my fork and change branch to `country-classifier`

Get the [dig-extractions-classifier](https://github.com/usc-isi-i2/dig-extractions-classifier) repo and put it in the same top level folder as etk

Follow etk setup

Inside `etk_env`


```python
conda install scikit-learn
conda install nltk
```

Change the config to point to local dictionaries




### Features - 

1. Country next to city - no. of cities (in that country) next to an extracted country within a distance of 3 (distance is calculated taking into account the segment too)
2. No. of countries extracted - no_of_explicit_mentions - no. of times a given country was extracted 
3. country next to state - no. of states (in that country) next to an extracted country within a distance of 3
4. No. of countries extracted from content_strict 
5. No. of countries extracted from url
6. No. of countries exracted from populated places - this calculates how many extracted cities are in the given country
7. colon next to city - boolean - checks if there's a colon next to any city (in that country)
only available after processing etk - 
8. Returns true if the city with the highest probability is the highest populated city in the given country

To be implemented - 
1. city next to state
2. use prob in more features


### ML To-do

* Balanced Trainining
* Feature normalization
* z-normalization on columns
* Try `LogisticRegressionCV` with `scoring='f1'`
