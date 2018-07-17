## Introduction

For each 'ifp_title'

1. Using spacy to extract entities from the title sentence
2. Selecting document(s) that contains those entities
3. Computing similarity between the 'ipf_title' and document title/sentences
4. write the output to file

## Installation

```
python -m spacy download en_core_web_lg
```



## Usage

1. Put the ifp_title file and new data under the 'resources' directory
2. Run the script 'ranking_pipeline.py' with the the input new data, ipf_title file, Mode = 'TITLE' or 'SENTENCE', and value of top k

```
python ranking_pipeline.py input_new_file_path ipf_tile_file_path Mode Top_K
```

For example:

```
python ranking_pipeline.py ./resources/new_2018-04-03-first-10000.jl ./resources/ifps_titles_test.txt TITLE 20
```



