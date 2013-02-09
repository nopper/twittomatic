Description
===========

- The `cleanup.py` script takes in inputs HTML files previously
downloaded from the extractor and transform in a flow of plain text.
- The `compute.py` script just computes all the n-grams up to 5-grams
for the input text. It creates a lot of `tsv.gz` files
- The `merger.py` script takes in input all the previously produced
`tsv.gz` files and creates a unique file that is the merge of all the
inputs.

How to use them
===============

    $ python cleanup.py ../splits/wikipedia-??.json.gz > plain-text.gz
    $ zcat plain-text.gz | python compute.py
    $ python merger.py > words-freq.gz
