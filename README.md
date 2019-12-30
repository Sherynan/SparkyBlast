# Sparky-Blast

Sparky-Blast is a new implementation of the Basic Local Alignment Search Tool (BLAST) algorithm that utilizes the Cassandra database to store the different reference datasets and the Apache Spark processing framework to calculate the indexes and process the queries.  Sparky-Blast is capable of using the distributed resources of a Big-Data Cluster to process queries in parallel, improving both the response time and system throughput. At the same time, the use of a distributed architecture like Hadoop gives the tool unlimited scalability from the point of view of both the hardware infrastructure and performance.

## Requeriments

Sparky-Blast requires a Spark cluster with the Cassandra database.

## Running it

### Create the sequence reference database

The firs step to execute Sparky-Blaset is create the sequence content database. This is done, using the *Blast_CreateReferenceContent.py* python program:

  Usage: Blast_CreateReferenceContent <Reference_Files> [ReferenceName=ReferenceFileName] [ContentBlockSize=1000]

It recibes 3 parameters: the fasta file with the reference sequences, the name for the Cassandra keyspace and the record block size.

```
./Blast_CreateReferenceContent.py GRCh38_latest_genomic.fna.gz grch38F 100
```


### Create the sequence reference Inverted-Index

The second step is to create the Inverted-Index in Cassandra for the reference sequences. It is done using the *SparkBlast_CreateReference.py* pyspark application:

Usage: SparkBlast_CreateReference <Reference_Files> [Key_size=11] [ReferenceName=blast] [Method=1] [BlockSize=128K] [ContentBlockSize=1000] [HashGroupSize=10000000] [StatisticsFileName]

This application is lauched using the spark-submit command. It can be specific some yarn attributes to define the resources assigned to the job (number of executors, number of cores by executor and the Driver & executor memory). The program recibes the sequence fasta file (this file has to be stored in HDFS), the key size (K), the reference keyspace name in cassandra, the method used for build the inverted-index, the input file block size, the content block size, the hash group size and the output stadistics file. 

```
spark-submit --name Hash_GRCh38F --master yarn --deploy-mode cluster --num-executors 5 --executor-cores 16  --driver-memory 8g --executor-memory 40g  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 ./SparkBlast_CreateReference.py /user/Datasets/References/GRCh38_latest_genomic.fna.gz 11 GRCh38F 1 $((128*1024)) 100000  120000000 ./Results/Scalability_GRCh38F.res

```

### Performing queries

The last step is to make the query against a reference sequence stored in Cassandra. It is done using the *SparkBlast_DoQuery* pyspark application:

# Usage: DoQuery [--MQuery] <Query_Files> <ReferenceName> [Key_size=11] [StadisticsFile] [NumberOfPartitions] [HashName] 

This application is also lauched using the spark-submit command. It can be specific some yarn attributes to define the resources assigned to the job (number of executors, number of cores by executor and the Driver & executor memory). The program recibes the method usesd (single query or multiple query processing), the query sequence fasta file (this file has to be stored in HDFS), the content reference keyspace name in cassandra, the key size (K) which has to match the one used to create the inverted index, the output stadistics file, the number of partitions for the procesing and the interted-index keyspace name in cassandra. 

```
spark-submit --name Query_GRCH38_5E16C --master yarn --deploy-mode cluster --num-executors 5 --executor-cores 16 --driver-memory 8g --executor-memory 40g --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 ./SparkBlast_DoQuery.py --MQuery /Datasets/Sequences/GRCh38_1K.csv.gz grch38F 11 ./Results/Scalability_DoQuery_GRCh38F.res 160 grch38F_
```


