#!/bin/bash

# Test coste creación Indice dee eferencia (Hash) para diferentes tamaños de la sequencia de referencia (10K, 20K hasta 100K lineas)

# Paths
SparkyBlast_Home = '.'
Dataset_Home = 'Datasets/Benchmarks/Small/'
References_Path = Dataset_Home+'/References/'
Querys_Path = Dataset_Home+'/Querys/'
Results_Path = Dataset_Home+'/Results/'
# Spark parameters
Spark_Master = 'yarn'
Spark_DeployMode = 'cluster'
Spark_Executors = 4
Spark_Executors_Cores = 2
Spark_Drive_Memory = '2g'
Spark_Executor_Memory = '8g'
# SparkyBlast parameters
SparkyBlast_KeySize = 21
SparkyBlast_BlockSize = $((128*1024))
SparkyBlast_Content_BlockSize = 100000
SparkyBlast_Hash_GroupSize = 120000000

spark-submit --name Hash_GRCh38_10K --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors --executor-cores $Spark_Executors_Cores \
--driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 SparkyBlast_Home+'/SparkBlast_CreateReference.py' \
$References_Path+'GRCh38_latest_genomic_10K.fna.gz' $SparkyBlast_KeySize GRCh38_10K 1 $SparkyBlast_BlockSize $SparkyBlast_Content_BlockSize \
$SparkyBlast_Hash_GroupSize  $Results_Path+'/Scalability_HashSize.res'

spark-submit --name Hash_GRCh38_20K --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors --executor-cores $Spark_Executors_Cores \
--driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 SparkyBlast_Home+'/SparkBlast_CreateReference.py' \
$References_Path+'GRCh38_latest_genomic_10K.fna.gz' $SparkyBlast_KeySize GRCh38_20K 1 $SparkyBlast_BlockSize $SparkyBlast_Content_BlockSize \
$SparkyBlast_Hash_GroupSize  $Results_Path+'/Scalability_HashSize.res'

spark-submit --name Hash_GRCh38_30K --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors --executor-cores $Spark_Executors_Cores \
--driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 SparkyBlast_Home+'/SparkBlast_CreateReference.py' \
$References_Path+'GRCh38_latest_genomic_10K.fna.gz' $SparkyBlast_KeySize GRCh38_30K 1 $SparkyBlast_BlockSize $SparkyBlast_Content_BlockSize \
$SparkyBlast_Hash_GroupSize  $Results_Path+'/Scalability_HashSize.res'

spark-submit --name Hash_GRCh38_40K --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors --executor-cores $Spark_Executors_Cores \
--driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 SparkyBlast_Home+'/SparkBlast_CreateReference.py' \
$References_Path+'GRCh38_latest_genomic_10K.fna.gz' $SparkyBlast_KeySize GRCh38_40K 1 $SparkyBlast_BlockSize $SparkyBlast_Content_BlockSize \
$SparkyBlast_Hash_GroupSize  $Results_Path+'/Scalability_HashSize.res'

spark-submit --name Hash_GRCh38_50K --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors --executor-cores $Spark_Executors_Cores \
--driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 SparkyBlast_Home+'/SparkBlast_CreateReference.py' \
$References_Path+'GRCh38_latest_genomic_10K.fna.gz' $SparkyBlast_KeySize GRCh38_50K 1 $SparkyBlast_BlockSize $SparkyBlast_Content_BlockSize \
$SparkyBlast_Hash_GroupSize  $Results_Path+'/Scalability_HashSize.res'

spark-submit --name Hash_GRCh38_60K --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors --executor-cores $Spark_Executors_Cores \
--driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 SparkyBlast_Home+'/SparkBlast_CreateReference.py' \
$References_Path+'GRCh38_latest_genomic_10K.fna.gz' $SparkyBlast_KeySize GRCh38_60K 1 $SparkyBlast_BlockSize $SparkyBlast_Content_BlockSize \
$SparkyBlast_Hash_GroupSize  $Results_Path+'/Scalability_HashSize.res'

spark-submit --name Hash_GRCh38_70K --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors --executor-cores $Spark_Executors_Cores \
--driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 SparkyBlast_Home+'/SparkBlast_CreateReference.py' \
$References_Path+'GRCh38_latest_genomic_10K.fna.gz' $SparkyBlast_KeySize GRCh38_70K 1 $SparkyBlast_BlockSize $SparkyBlast_Content_BlockSize \
$SparkyBlast_Hash_GroupSize  $Results_Path+'/Scalability_HashSize.res'

spark-submit --name Hash_GRCh38_80K --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors --executor-cores $Spark_Executors_Cores \
--driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 SparkyBlast_Home+'/SparkBlast_CreateReference.py' \
$References_Path+'GRCh38_latest_genomic_10K.fna.gz' $SparkyBlast_KeySize GRCh38_80K 1 $SparkyBlast_BlockSize $SparkyBlast_Content_BlockSize \
$SparkyBlast_Hash_GroupSize  $Results_Path+'/Scalability_HashSize.res'

spark-submit --name Hash_GRCh38_90K --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors --executor-cores $Spark_Executors_Cores \
--driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 SparkyBlast_Home+'/SparkBlast_CreateReference.py' \
$References_Path+'GRCh38_latest_genomic_10K.fna.gz' $SparkyBlast_KeySize GRCh38_90K 1 $SparkyBlast_BlockSize $SparkyBlast_Content_BlockSize \
$SparkyBlast_Hash_GroupSize  $Results_Path+'/Scalability_HashSize.res'

spark-submit --name Hash_GRCh38_100K --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors --executor-cores $Spark_Executors_Cores \
--driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory  --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 SparkyBlast_Home+'/SparkBlast_CreateReference.py' \
$References_Path+'GRCh38_latest_genomic_10K.fna.gz' $SparkyBlast_KeySize GRCh38_100K 1 $SparkyBlast_BlockSize $SparkyBlast_Content_BlockSize \
$SparkyBlast_Hash_GroupSize  $Results_Path+'/Scalability_HashSize.res'

