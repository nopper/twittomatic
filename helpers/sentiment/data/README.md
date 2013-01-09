This directory must contains the following files:

  - SentiWordNet_3.0.0.txt
  - morph-it.txt
  - mwnet.db

SentiWordNet can be downloaded directly from the official
[SentiWordNet](http://sentiwordnet.isti.cnr.it/) site.

The same applies to Morph-it! that can be downloaded from the
[SSLMIT Dev Site](http://dev.sslmit.unibo.it/linguistics/morph-it.php).

The mwnet.db file needs to be created starting from
(MultiWordNet database)[http://multiwordnet.fbk.eu/english/home.php].

You simply need to execute the following commands:

    $ sqlite3 mwnet.db < italian_index.sql
    $ sqlite3 mwnet.db < english_synset.sql
