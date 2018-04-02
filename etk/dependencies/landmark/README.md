This package if forked from https://github.com/inferlink/landmark-extractor 

I have upgraded some syntax to use in Python3, and removed some not used files.

# Landmark Extraction

## Running
No third party libraries are necessary. Run the following from the command line
``` bash
python -m landmark_extractor.extraction.Landmark <OPTIONAL_PARAMS> <FILE_TO_EXTRACT_FROM> <RULES_FILE>
```

`<OPTIONAL_PARAMS>`

`-f`: Flatten the extractions

### Examples
```bash
python -m landmark_extractor.extraction.Landmark sample/jair1.html sample/jair_rules.json
```
```json
{
  "title": {
    "extract": "Developing Approaches for Solving a Telecommunications Feature Subscription Problem",
    "begin_index": 2804,
    "end_index": 2888
  },
  "abstract": {
    "extract": "Call control features (e.g., call-divert, voice-mail) are primitive options to which users can subscribe off-line to personalise their service. The configuration of a feature subscription involves choosing and sequencing features from a catalogue and is subject to constraints that prevent undesirable feature interactions at run-time. When the subscription requested by a user is inconsistent, one problem is to find an optimal relaxation, which is a generalisation of the feedback vertex set problem on directed graphs, and thus it is an NP-hard task. We present several constraint programming formulations of the problem. We also present formulations using partial weighted maximum Boolean satisfiability and mixed integer linear programming. We study all these formulations by experimentally comparing them on a variety of randomly generated instances of the feature subscription problem.",
    "begin_index": 1784,
    "end_index": 2684
  },
  "authors": {
    "sequence": [
      {
        "extract": "Lesaint, D.",
        "begin_index": 2929,
        "sequence_number": 1,
        "end_index": 2940
      },
      {
        "extract": "Mehta, D.",
        "begin_index": 2981,
        "sequence_number": 2,
        "end_index": 2990
      },
      {
        "extract": "O'Sullivan, B.",
        "begin_index": 3031,
        "sequence_number": 3,
        "end_index": 3045
      },
      {
        "extract": "Quesada, L.",
        "begin_index": 3086,
        "sequence_number": 4,
        "end_index": 3097
      },
      {
        "extract": "Wilson, N.",
        "begin_index": 3138,
        "sequence_number": 5,
        "end_index": 3148
      }
    ],
    "extract": " name=\"citation_title\" content=\"Developing Approaches  for Solving a Telecommunications Feature Subscription Problem\">\n<meta name=\"citation_author\" content=\"Lesaint, D.\">\n<meta name=\"citation_author\" content=\"Mehta, D.\">\n<meta name=\"citation_author\" content=\"O'Sullivan, B.\">\n<meta name=\"citation_author\" content=\"Quesada, L.\">\n<meta name=\"citation_author\" content=\"Wilson, N.\">\n<meta name=\"citation_publication_date\" content=\"2010\">\n<meta name=\"citation_journal_title\" content=\"Journal of Artificial Intelligence Research\">\n<meta name=\"citation_firstpage\" content=\"271\">\n<meta name=\"citation_lastpage\" content=\"305\">\n<meta name=\"citation_pdf_url\" content=\"http://www.jair.org/media/2992/live-2992-5030-jair.pdf\">\n",
    "begin_index": 2772,
    "end_index": 3486
  },
  "volume": {
    "extract": "2010",
    "begin_index": 3199,
    "end_index": 3203
  },
  "pages": {
    "extract": "271-305",
    "begin_index": 1535,
    "end_index": 1542
  }
}
```
```bash
python -m landmark_extractor.extraction.Landmark -f sample/jair1.html sample/jair_rules.json
```
```json
{
  "volume": "2010",
  "abstract": "Call control features (e.g., call-divert, voice-mail) are primitive options to which users can subscribe off-line to personalise their service. The configuration of a feature subscription involves choosing and sequencing features from a catalogue and is subject to constraints that prevent undesirable feature interactions at run-time. When the subscription requested by a user is inconsistent, one problem is to find an optimal relaxation, which is a generalisation of the feedback vertex set problem on directed graphs, and thus it is an NP-hard task. We present several constraint programming formulations of the problem. We also present formulations using partial weighted maximum Boolean satisfiability and mixed integer linear programming. We study all these formulations by experimentally comparing them on a variety of randomly generated instances of the feature subscription problem.",
  "authors": [
    "Lesaint, D.",
    "Mehta, D.",
    "O'Sullivan, B.",
    "Quesada, L.",
    "Wilson, N."
  ],
  "pages": "271-305",
  "title": "Developing Approaches for Solving a Telecommunications Feature Subscription Problem"
}
```

## CREATING RULES
There are two types of rules currently that can be used to extract information from text. They are highlighted below:

### RegexRule - Used to extract one piece of content from the text
```
{
    "name": "title",
    "rule_type": "RegexRule",
    "begin_regex": "<meta name=\"citation_title\" content=\"",
    "end_regex": "\">"
}
```
* name: the name of this rule
* rule_type: RegexRule
* begin_regex: The quote escaped regular expression to get to the beginning of where you would like to extract.
* end_regex: The quote escaped regular expression to get to the end of where you would like to extract (starting from the end of begin_regex).

### RegexIterationRule - Used to extract a list of content from the text
```
{
    "name": "authors",
    "rule_type": "RegexIterationRule",
    "begin_regex": "<meta",
    "end_regex": "</div>",
    "iter_begin_regex": "citation_author\" content=\"",
    "iter_end_regex": "\">",
    "no_first_begin_iter_rule": true,
    "no_last_end_iter_rule": false
}
```
* name: the name of this rule
* rule_type: RegexRule
* begin_regex: The quote escaped regular expression to get to the beginning of where you would like to extract, not included in extraction.
* end_regex: The quote escaped regular expression to get to the end of where you would like to extract (starting from the end of begin_regex).  Excluded from extraction by default.
* iter_begin_regex: The quote escaped regular expression for the beginning of EACH item to be repeated, not included in extraction.  Or if iter_end_regex is not provided, matches up until the end of EACH item to be repeated.
* iter_end_regex [Optional]: The quote escaped regular expression for the end EACH item to be repeated, not included in extraction (starting from the end of each iter_begin_regex).
* no_first_begin_iter_rule [Optional]: Boolean which defines if the iter_begin_regex should be used for the FIRST element of the list
* no_last_end_iter_rule [Optional]: Boolean which defines if the iter_end_regex should be used for the LAST element of the list

# Landmark Validation

## Running
No third party libraries are necessary. Run the following from the command line
``` bash
python -m landmark_extractor.validation.Validation <OPTIONAL_PARAMS> <VALIDATION_RULES_FILE> <EXTRACTIONS>
```

`<OPTIONAL_PARAMS>`

`-s`: Simplify results

### Examples
```bash
python -m landmark_extractor.validation.Validation -s sample/validation_rules_sample_valid.json sample/sample_extrations.json
```
```json
{
  "valid": true
}
```

```bash
python -m landmark_extractor.validation.Validation -s sample/validation_rules_sample_invalid.json sample/sample_extrations.json
```
```json
{
  "valid": false
}
```