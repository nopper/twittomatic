#!/bin/sh
echo "Extracting titles..."
gzcat ../wiki-title.json.gz | python extract-titles.py

echo "Extracting redirects from DBPedia..."
bzcat redirects_it.nt.bz2 | python extract-redirects.py

echo "Extracting out-links..."
gzcat ../outlinks.json.gz | python convert-outlinks.py | sort -n -k1 -k2 | gzip -c > out-rels.adj.gz

echo "Inverting out-links to generate in-links"
gzcat out-rels.adj.gz | awk '{print $2 "\t" $1}' | sort -n -k1 -k2 | gzip -c > in-rels.adj.gz

echo "Packing outlinks..."
gzcat out-rels.adj.gz | python pack-rels.py outdeg
echo "Packing inlinks..."
gzcat in-rels.adj.gz | python pack-rels.py indeg

echo "Extracting anchors..."
gzcat ../anchors/anchors.gz | python extract-anchors.py > anchors

echo "Extracting disambiguation anchors from DBPedia..."
bzcat disambiguations_it.nt.bz2 | python extract-missing-anchors.py >> anchors

echo "Sorting anchors..."
sort -k1,1 -k2,2n -k3,3n anchors > anchors-sorted

echo "Packing anchors..."
cat anchors-sorted | python pack-anchors.py

echo "Inserting link-probability..."
cat ../anchors/anchors-lp.txt| python pack-anchors-lps.py
