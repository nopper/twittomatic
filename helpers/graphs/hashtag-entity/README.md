How to get the graph
====================

After the annotation with TagME or lightTAG, use the `extract.py` in conjunction with a `sort -k1` pipe to create a sorted list of edges:

    $ python extract.py -i annotations-0.4.json.gz | sort -k1 | gzip -c > he-edges.tsv.gz

After that you can obtain a complete graph with the `render.py` script:

    $ python render.py --skip-single -i he-edges.tsv.gz -o he-graph.tsv.gz

The format of the file is

    [Hashtag-id][TAB][WIKIPEDIA ID + 100000000][TAB][rho1:rho2:...][TAB][HASHTAG][TAG][WIKIPEDIA TITLE]

To get an `.adj` file just pipe the output with `awk`:

    $ zcat he-graph.tsv.gz | awk '{print $1 "\t" $2} > he-graph.adj

Extract GraphML file
====================

First we need to get the list of all nodes partecipating in the graph:


    $ zcat he-graph.tsv.gz | awk '{print $1 "\t" $4}' | uniq > hashtag-ids.txt
    $ zcat he-graph.tsv.gz | awk '{print $2 "\t" $5}' | sort -n -k1 -u > wikipedia-ids.txt

After that extraction you can simply run the `graphml-render.py` script:

    $ python render-graphml.py \
        --ht-nodes hashtag-ids.txt \
        --wiki-nodes wikipedia-ids.txt \
        -i he-graph.tsv.gz -o he-graph.xml.gz
