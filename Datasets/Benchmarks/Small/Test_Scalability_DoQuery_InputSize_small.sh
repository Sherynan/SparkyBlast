#!/bin/bash

# Test escalabilidad de las prestaciones a medida que incrementamos el tamaño de la query (100, 500, 1K, 2k,.., 8K, 10K, 20K consultas) para una misma referencia (GRCh38_10K)

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
SparkyBlast_Reference_Hash_Name = 'GRCh38_20K'
SparkyBlast_Partitions = 80
SparkyBlast_Results_File = $Results_Path+'/Scalability_InputSizeDoQuery.res'

spark-submit --name Query_GRCH38_K21_100 --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $Querys_Path+'/GRCh38_latest_genomic_100.csv.gz' $SparkyBlast_Reference_Hash_Name $SparkyBlast_KeySize \
$SparkyBlast_Results_File $SparkyBlast_Partitions $SparkyBlast_Reference_Hash_Name

spark-submit --name Query_GRCH38_K21_500 --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $Querys_Path+'/GRCh38_latest_genomic_500.csv.gz' $SparkyBlast_Reference_Hash_Name $SparkyBlast_KeySize \
$SparkyBlast_Results_File $SparkyBlast_Partitions $SparkyBlast_Reference_Hash_Name

spark-submit --name Query_GRCH38_K21_1K --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $Querys_Path+'/GRCh38_latest_genomic_1K.csv.gz' $SparkyBlast_Reference_Hash_Name $SparkyBlast_KeySize \
$SparkyBlast_Results_File $SparkyBlast_Partitions $SparkyBlast_Reference_Hash_Name

spark-submit --name Query_GRCH38_K21_2K --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $Querys_Path+'/GRCh38_latest_genomic_2K.csv.gz' $SparkyBlast_Reference_Hash_Name $SparkyBlast_KeySize \
$SparkyBlast_Results_File $SparkyBlast_Partitions $SparkyBlast_Reference_Hash_Name

spark-submit --name Query_GRCH38_K21_4K --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $Querys_Path+'/GRCh38_latest_genomic_4K.csv.gz' $SparkyBlast_Reference_Hash_Name $SparkyBlast_KeySize \
$SparkyBlast_Results_File $SparkyBlast_Partitions $SparkyBlast_Reference_Hash_Name

spark-submit --name Query_GRCH38_K21_6K --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $Querys_Path+'/GRCh38_latest_genomic_6K.csv.gz' $SparkyBlast_Reference_Hash_Name $SparkyBlast_KeySize \
$SparkyBlast_Results_File $SparkyBlast_Partitions $SparkyBlast_Reference_Hash_Name

spark-submit --name Query_GRCH38_K21_8K --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $Querys_Path+'/GRCh38_latest_genomic_8K.csv.gz' $SparkyBlast_Reference_Hash_Name $SparkyBlast_KeySize \
$SparkyBlast_Results_File $SparkyBlast_Partitions $SparkyBlast_Reference_Hash_Name

spark-submit --name Query_GRCH38_K21_10K --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $Querys_Path+'/GRCh38_latest_genomic_10K.csv.gz' $SparkyBlast_Reference_Hash_Name $SparkyBlast_KeySize \
$SparkyBlast_Results_File $SparkyBlast_Partitions $SparkyBlast_Reference_Hash_Name

spark-submit --name Query_GRCH38_K21_20K --master $Spark_Master --deploy-mode $Spark_DeployMode --num-executors $Spark_Executors \
--executor-cores $Spark_Executors_Cores --driver-memory $Spark_Drive_Memory --executor-memory $Spark_Executor_Memory --packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1 \
$SparkyBlast_Home+'/SparkBlast_DoQuery.py' --MQuery $Querys_Path+'/GRCh38_latest_genomic_20K.csv.gz' $SparkyBlast_Reference_Hash_Name $SparkyBlast_KeySize \
$SparkyBlast_Results_File $SparkyBlast_Partitions $SparkyBlast_Reference_Hash_Name

