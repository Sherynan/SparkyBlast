# Sparky-Blast

Sparky-Blast is a new implementation of the Basic Local Alignment Search Tool (BLAST) algorithm that utilizes the Cassandra database to store the different reference datasets and the Apache Spark processing framework to calculate the indexes and process the queries.  Sparky-Blast is capable of using the distributed resources of a Big-Data Cluster to process queries in parallel, improving both the response time and system throughput. At the same time, the use of a distributed architecture like Hadoop gives the tool unlimited scalability from the point of view of both the hardware infrastructure and performance.

## Requeriments

Sparky-Blast requires a Spark cluster with the Cassandra database.

## Running it

### Create the sequence reference database

The firs step to execute Sparky-Blaset is create the sequence content database. This is done, using the Blast_CreateReferenceContent.py python program:

Usage: Blast_CreateReferenceContent <Reference_Files> [ReferenceName=ReferenceFileName] [ContentBlockSize=1000]

'''
./Blast_CreateReferenceContent.py GRCh38_latest_genomic.fna.gz grch38F 100
'''

It recibes 3 parameters: the fasta file with the reference sequences, the name for 

### Create the sequence reference Inverted-Index

### Performing queries




