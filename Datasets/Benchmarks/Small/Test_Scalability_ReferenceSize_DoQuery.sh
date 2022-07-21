#!/bin/bash

# Test escalabilidad de tiempo requrido para realizar una query (1K consultas) para diferentes tamaños de la sequencia de referencia (10K, 20K hasta 100K lineas)
# Paths
SparkyBlast_Home = '.'
Dataset_Home = 'Datasets/Benchmarks/Small/'
References_Path = Dataset_Home+'/References/'
Querys_Path = Dataset_Home+'/Querys/'
Results_Path = Dataset_Home+'/Results/'
# Spark parameters
Spark_Master = 'yarn'
Spark_DeployMode = 'cluster'
Spark_Executors = 2
Spark_Executors_Cores = 4
Spark_Drive_Memory = '2g'
Spark_Executor_Memory = '8g'
# SparkyBlast parameters
SparkyBlast_KeySize = 21
SparkyBlast_BlockSize = $((128*1024))
SparkyBlast_Content_BlockSize = 100000
SparkyBlast_Hash_GroupSize = 120000000
SparkyBlast_Query_File = $Querys_Path+'/GRCh38_latest_genomic_1K.csv.gz'
SparkyBlast_Results_File = $Results_Path+'/Scalability_ReferenceSizeDoQuery.res'

spark-submit --name Query_1K_GRCH38_10K_K21_100 --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $SparkyBlast_Query_File GRCh38_10K $SparkyBlast_KeySize \
SparkyBlast_Results_File $SparkyBlast_Partitions GRCh38_10K

spark-submit --name Query_1K_GRCH38_20K_K21_100 --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $SparkyBlast_Query_File GRCh38_20K $SparkyBlast_KeySize \
SparkyBlast_Results_File $SparkyBlast_Partitions GRCh38_20K

spark-submit --name Query_1K_GRCH38_30K_K21_100 --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $SparkyBlast_Query_File GRCh38_30K $SparkyBlast_KeySize \
SparkyBlast_Results_File $SparkyBlast_Partitions GRCh38_30K

spark-submit --name Query_1K_GRCH38_40K_K21_100 --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $SparkyBlast_Query_File GRCh38_40K $SparkyBlast_KeySize \
SparkyBlast_Results_File $SparkyBlast_Partitions GRCh38_40K

spark-submit --name Query_1K_GRCH38_50K_K21_100 --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $SparkyBlast_Query_File GRCh38_50K $SparkyBlast_KeySize \
SparkyBlast_Results_File $SparkyBlast_Partitions GRCh38_50K

spark-submit --name Query_1K_GRCH38_60K_K21_100 --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $SparkyBlast_Query_File GRCh38_60K $SparkyBlast_KeySize \
SparkyBlast_Results_File $SparkyBlast_Partitions GRCh38_60K

spark-submit --name Query_1K_GRCH38_70K_K21_100 --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $SparkyBlast_Query_File GRCh38_70K $SparkyBlast_KeySize \
SparkyBlast_Results_File $SparkyBlast_Partitions GRCh38_70K

spark-submit --name Query_1K_GRCH38_80K_K21_100 --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $SparkyBlast_Query_File GRCh38_80K $SparkyBlast_KeySize \
SparkyBlast_Results_File $SparkyBlast_Partitions GRCh38_80K

spark-submit --name Query_1K_GRCH38_90K_K21_100 --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $SparkyBlast_Query_File GRCh38_90K $SparkyBlast_KeySize \
SparkyBlast_Results_File $SparkyBlast_Partitions GRCh38_90K

spark-submit --name Query_1K_GRCH38_100K_K21_100 --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $SparkyBlast_Query_File GRCh38_100K $SparkyBlast_KeySize \
SparkyBlast_Results_File $SparkyBlast_Partitions GRCh38_100K
