Internet Archive Data Processing
================================

This package is a multi threading pipeline for processing graph data.
Due to large quantity of the graph data we have requirement of
multiprocessing in the code. Currently the code uses the maximum
available core to the user.


Dependency
==========
[DNS Python][dp]
[dp]: http://www.dnspython.org/

Static Sources
--------------
[Country Code To TLD Mapping][cctld]
[cctld]: http://en.wikipedia.org/wiki/ISO_3166-1_alpha-2


Code Execution
==============
Update the location of the input, output file in global.py
Also the code requires a intermediate directory for file splitting
and aggregation which can be updated in global.py file.

python pipeline\_out.py

Modules
=======

globals
-------
Maintains the location for input and output.
It also provides API for generating unique intermediate file locations.
which are useful during various merge operations.

pipeline\_out
------------
This is the main executer of code. It calls each of the processing
step and finally Aggregates the files to create data acceptable by 
the database seeder.

process\_manager
-------------
This code extends the multiprocessing to create API from generating multiple instance.
On a instance of process manager we can add jobs using jobs\_queue and we need to wait
for the jobs to be completed.

basic\_extraction
-----------------

BasicExtractionManger

Generates multiple instances of BasicExtraction using process manager.

BasicExtraction

Each instance takes a file as input and extracts hostname from the url.
and creates a separate output just for the nodes.

merge\_files
------------

MergeFilesManager

Calls multiple instances of MergeFiles. Till all the nodes are merged together.

MergeFiles

Takes any two files as input and runs the merge operation.

Design
======

Code as a pipeline in which each step completes before next
step starts.

Major steps :-

a) Basic Extraction -  The Basic extraction step aggregates
the data from different war files. It parses url and rejects
dirty url links. It then aggregates the edges. Also it divides
the data into node files and edge file.

b) Merge File :- This step we merge all the data using merge sort.
This also allows us to aggregate edge count across files.

c) Domain Mapper :-  For each url we extract the domain of the file
and try to classify into the top level domains.

d) TLD Mapping :- We try to generate the country associated with 
		 url. This is done first using [tld][cctld] then using
		 ip region lookup.

