# SparkBlast_DoQuery program to perform blast querys on cassandra using spark
# Usage: SparkBlast_DoQuery <Query_Files> <ReferenceName> [Key_size=11].

from __future__ import print_function
import pyspark
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import Row, _infer_schema, _has_nulltype, _merge_type, _create_converter
from pyspark.sql.types import StringType, ArrayType, LongType, StructType, StructField, IntegerType
from pyspark.sql.functions import udf, upper, desc, collect_list, size, mean, length
from cassandra.cluster import Cluster
from operator import add
import re
from time import time, sleep
import os, shutil, sys, subprocess
import traceback
from subprocess import PIPE, Popen
import random
from datetime import datetime
import collections
from collections import defaultdict
import cython
import math



## Constants
DCassandraNodes = ['192.168.1.1', '192.168.1.2', '192.168.1.3', '192.168.1.4', '192.168.1.5', '192.168.1.6']
#DCassandraNodes = ['192.168.1.3']
DDoTesting = False
DDebug = False
DTiming = True
DShowResult = False
DSaveResult = False
APP_NAME = "SparkBlast_DoQuery"
DKeySize = 11
DQueryFilename = '../Datasets/References/Query1.txt'
DReferenceName = "example"
DReferenceHashTableName = "hash"
DReferenceContentTableName = "sequences"
DCreateWindowWithPartitions = True
DCreateBlocksDataFrame = True
DPartitionBlockSize = 128  * 1024 
DProcessingByPartitions = True
DNumberPartitions = 200
DMaxNumberStages = 3
DBalanceGetKeysOffsesPartitions = True
DBalanceAligmentsCalculation = False
DAligmentMaxNumberStages = 10
DMaxOffsetsQuery = 5
DDoMultipleQuery = False
DMinAligmentScore = 0.7
DAligmentExtension = False
#DAligmentExtensionLength = 15
DAligmentExtensionLength = 6
DBlockCacheSize = 3
DCythonLibsPath = 'hdfs://babel.udl.cat/user/nando/cython_libs/'
#DCythonLibsPath = '/tmp/cython_libs/'
cyt_calculateMultipleQueryKeysDesplR = None

# Error Handling
DCassandraRetriesNumber = 5
DCassandraRetryTimeout = 100/1000000.0 # 100 microsecs

# Statistics 
DHdfsHomePath = "hdfs://babel.udl.cat/user/nando/"
DHdfsTmpPath = DHdfsHomePath + "Tmp/"
DHdfsOutputPath = DHdfsHomePath + "Output/"
DCalculateStageStatistics = False
DCalculateStatistics = True

## Types
# Enum Methods:
ECreate2LinesData = 1
ECreate1LineDataWithoutDependencies = 2
ECreateBlocksData = 3
DDefaultMethod = ECreate2LinesData
    

## Global Variables
Method = DDefaultMethod
CreateWindowWithPartitions = DCreateWindowWithPartitions
BlockSize = DPartitionBlockSize
DoMultipleQuery = DDoMultipleQuery
NumberPartitions = DNumberPartitions
DKeyMatchingThreshold = 15
KeyMatchingThreshold = DKeyMatchingThreshold
DKeyMatchingPercentageThreshold = 0.25
StatisticsFileName = None
YarnJobId = None
key_size_bc = 0
query_length_bc = 0
gt0_bc = time()
reference_name_bc = 0
query_sequence_bc = 0
content_block_size_bc = 0
keysdespl_rdd = 0
reference_keys = 0



## Functions ##
###############


#
# Single query processing method.
#
def Query(sc, sqlContext, queryFilename, referenceName, keySize=DKeySize, hashName=None):
    dfc("Query", sc, sqlContext, queryFilename, referenceName, keySize, hashName)
    
    # Broadcast Global variables
    global DDebug, key_size_bc, gt0_bc, reference_name_bc, hash_name_bc
    t0 = time()
    gt0_bc = sc.broadcast(t0)
    key_size_bc = sc.broadcast(keySize)
    reference_name_bc = sc.broadcast(referenceName)
    hash_name_bc = sc.broadcast(hashName)
    
    if (DTiming):
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y %H:%M:%S")
        executors = sc._conf.get("spark.executor.instances")
        cores = sc.getConf().get("spark.executor.cores")
        memory = sc.getConf().get("spark.executor.memory")
        print("++++++++++++ INITIAL STATISTICS {} +++++++++++++".format(date_time))
        print("+ Reference: {}  \tQuery file: {}.".format(referenceName, queryFilename))
        print("+ Key Size: {}   \tMethod: {}.".format(keySize, Method))
        print("+ Num Executors: {}  \tExecutors/cores: {}  \tExecutor Mem: {}.".format(executors, cores, memory))
        #print("+ Hash Groups Size: {}  \tPartition Size: {}  \tContent Block Size: {}.".format(HashGroupsSize, BlockSize, ContentBlockSize))
        print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    
    # Create Cassandra Sesion
    for i in range(0,DCassandraRetriesNumber):
        while True:
            try:
                cluster = Cluster(DCassandraNodes)
                session = cluster.connect()
            except:
                # Print exception info.
                print("@@@@@ Capatured Exception")
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                del exc_info
                # Retry operation after a delay.
                sleep(DCassandraRetryTimeout)
                continue
            break
    if session is None:
        raise  
 
    # Steps:
    #   1. Read and split query in keySize-Segments + Desplazament
    #   2. Query Cassandra for the keys
    #   3. Calculate Top-matching zones
    #   4. Make extension in Top-matching zones.
    

    # 1. Read and split query in keySize-Segments + Desplazament
    global keysdespl_rdd, reference_keys
    t1 = time()
    keysdespl_rdd, query_sequence = CalculateQueryOffset(sc, queryFilename, keySize)
    t_read = time()-t1
    
    if (DDebug):
        print("Query::QueryKeys:")
        keysdespl_rdd.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        print(keysdespl_rdd.take(1))
        
    # 2. Query Cassandra for the keys
    t2 = time()
    if (DProcessingByPartitions):
        reference_keys = GetKeysOffsetsInReferenceByPartition(sc, session, referenceName, keysdespl_rdd)
    else:
        reference_keys = GetKeysOffsetsInReference(sc, session, referenceName, keysdespl_rdd)
    t_qcass = time()-t2
        
    if (DDebug):
        print("Query::QueryKeys: Matching desplazaments: {}".format(reference_keys.take(1)))
        
          
    # 3. Calculate Top-matching offsets
    t3 = time()
    offsets_count = reference_keys.map(lambda off: (off,1)).reduceByKey(add)
    top_offsets = offsets_count.sortBy(lambda kv: kv[1], False)
    # Get Top-matching zones that exceed the threshold.
    top_matching = offsets_count.filter(lambda kv: kv[1]>DKeyMatchingThreshold) 
    top_matchng.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    candidate_offsets = top_matching.count()
    t_topmat = time()-t3
    
    if (DDebug):
        print("Top-10 offsets: {}".format(top_offsets.take(1)))
        print("Top-Matching offsets: {}".format(top_matching.take(1)))  
      
    # 4. Make extension in Top-matching zones.
    t4 = time()
    good_aligments = CalculateAligments(sc, session, query_sequence, referenceName, top_matching)
    tc = time()
    good_aligments.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    n_aligments = good_aligments.count()
    t_ext = tc-t4
    tt = tc - t0

    if (DTiming):
        print("# Time required for calculate {} aligments: {} seconds.\n".format(n_aligments,round(tt,3)))
    
    if (DDebug):
        print("Good Aligments:")
        print(good_aligments.take(1))
        
    if (DShowResult):
        map(ShowAligmentResult,good_aligments.collect())

    if (DSaveResult):
        global YarnJobId
        ResultFile = DHdfsOutputPath+APP_NAME+"_"+ os.path.splitext(os.path.basename(queryFilename))[0] +"_"+referenceName+"_"+YarnJobId
        print("Writing matching aligments in file {}".format(ResultFile))
        good_aligments.coalesce(1).saveAsTextFile(ResultFile)

    session.shutdown()
    cluster.shutdown()
               
    if (DTiming):
        print("# Total time required for processing the query with {} keys: {} seconds.".format(keysdespl_rdd.count(), round(tt,3)))

        executors = sc._conf.get("spark.executor.instances")
        cores = sc.getConf().get("spark.executor.cores")
        memory = sc.getConf().get("spark.executor.memory")
        print("########################### FINAL STATISTICS Single QUERY {} ###########################".format(date_time))
        print("# Reference: {}  \tFile: {}   \tQuerys: {}.".format(ReferenceName, queryFilename, 1))
        print("# Key Size: {}            \tMethod: {}.".format(KeySize, Method))
        print("# Num Executors: {}      \tExecutors/cores: {}      \tExecutor Mem: {}.".format(executors, cores, memory))
        #print("# Hash Groups Size: {}  \tPartition Size: {}  \tContent Block Size: {}.".format(HashGroupsSize, BlockSize, ContentBlockSize))
        print("# Total Time: {}         \tData Read Time: {}       \tCassandra Read Time: {} .".format(round(tt,3), round(t_read,3), round(t_qcass,3)))
        print("# Top matching Time: {}   \tAlig. Extension Time: {}.".format(round(t_topmat,3), round(t_ext,3)))
        print("############################################################################################")
        #result.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        #print("# Total time required for processing {} keys using {} partitions in {} seconds.".format(result.count(), result.getNumPartitions(), round(tt,3)))
        #print("# Reference data size: {} MBytes.\n".format(round(get_size(ReferenceFilename)/(1024.0*1024.0),3)))
        if (StatisticsFileName):
            write_statistics_single_query(StatisticsFileName, queryFilename, ReferenceName, KeySize, date_time, 1, candidate_offsets, n_aligments, offsets_count.getNumPartitions() ,tt, t_read, t_qcass, t_topmat, t_ext)
        
    print("Done.")
        
    return good_aligments


def pquerykeys(qk):
    res = []
    for off in qk[1]:
        res.append((qk[0], off[0], off[1])) 
    return ([x for x in res])

def addDespl(off_list,despl):
    res = [off + despl for off in off_list] 
    return res
    
def flatten(off_list):
    return ([y for x in off_list for y in x])

def countby(off_list):
    return(collections.Counter(off_list).items())

def countbyfilter(off_list):
    return(filter(lambda (k,v): v>DKeyMatchingThreshold,collections.Counter(off_list).items()))

def countbyfilterdespl(off_list):
    return(map(lambda (k,v): k, filter(lambda (k,v): v>DKeyMatchingThreshold,collections.Counter(off_list).items())))


#
# Multiple queries processing method.
#
def MultipleQuery(sc, sqlContext, queryFilename, referenceName, keySize=DKeySize, hashName=None):
    global DDebug
    if (DDebug):
        print("MultipleQuery({}, {}, {}).".format( queryFilename, referenceName, keySize, hashName))
    dfc("MultipleQuery",sc, sqlContext, queryFilename, referenceName, keySize, hashName)
    
    # Broadcast Global variables
    global key_size_bc, gt0_bc, reference_name_bc, hash_name_bc
    t0 = time()
    gt0_bc = sc.broadcast(t0)
    key_size_bc = sc.broadcast(keySize)
    reference_name_bc = sc.broadcast(referenceName)
    hash_name_bc = sc.broadcast(hashName)

    nquerys = -1
    nkeysdespl = -1
    candidates_offsets = -1
    npartitions = -1
    n_aligments = -1

    if (DTiming):
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y %H:%M:%S")
        executors = sc._conf.get("spark.executor.instances")
        cores = sc.getConf().get("spark.executor.cores")
        memory = sc.getConf().get("spark.executor.memory")
        print("++++++++++++ INITIAL STATISTICS {} +++++++++++++".format(date_time))
        print("+ Reference: {}  \tQuery file: {}.".format(ReferenceName, queryFilename))
        print("+ Key Size: {}   \tMethod: {}.".format(KeySize, Method))
        print("+ Num Executors: {}  \tExecutors/cores: {}  \tExecutor Mem: {}.".format(executors, cores, memory))
        #print("+ Hash Groups Size: {}  \tPartition Size: {}  \tContent Block Size: {}.".format(HashGroupsSize, BlockSize, ContentBlockSize))
        print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    
    # Create Cassandra Sesion
    for i in range(0,DCassandraRetriesNumber):
        while True:
            try:
                cluster = Cluster(DCassandraNodes)
                session = cluster.connect()
            except:
                # Print exception info.
                print("@@@@@ Capatured Exception")
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                del exc_info
                # Retry operation after a delay.
                sleep(DCassandraRetryTimeout)
                continue
            break
    if session is None:
        raise


  
    # 1. Read and split query in keySize-Segments + Desplazament
    global keysdespl_rdd, reference_keys
    t1 = time()
    keysdespl_rdd, query_sequence = CalculateMultipleQueryOffset(sc, sqlContext, queryFilename, keySize)
    if (DCalculateStageStatistics):
        query_sequence.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        keysdespl_rdd.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        nquerys = 0
        if (query_sequence!=sc.emptyRDD()):
            nquerys = query_sequence.count()
        nkeysdespl = 0
        if (keysdespl_rdd!=sc.emptyRDD()):
            nkeysdespl = keysdespl_rdd.count()
        print("@@@@@ nkeysdespl: {}".format(nkeysdespl))
    t_read = time()-t1
    print("@@@@@ 1-Read and split query: {}".format(round(t_read,3)))  
    #print(keysdespl_rdd.take(100))

    
    if (DDebug):
        print("######################### Number of partitions query_sequence: {}".format(query_sequence.getNumPartitions()))
        print("######################### Number of partitions keysdespl_rdd: {}".format(keysdespl_rdd.getNumPartitions()))
        keysdespl_rdd.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        print("Query::QueryKeys:")
        print(keysdespl_rdd.take(1))
        print(query_sequence.take(1))
   
   
    # 2. Query Cassandra for the keys
    t2 = time()
    reference_keys = GetMultipleKeysOffsetsInReference(sc, session, referenceName, keysdespl_rdd)
    if (DCalculateStageStatistics):    
        reference_keys.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        print("@@@@@ Reference_keys: {}".format(reference_keys.count()))     
    t_qcass = time()-t2
    print("@@@@@ 2-Query Cassandra for {} keys: {}".format(reference_keys.count(), round(t_qcass,3)))  
        
    if (DDebug):
        reference_keys.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        print("######################### Number of partitions reference_keys: {}".format(reference_keys.getNumPartitions()))
        print("Query::QueryKeys: Matching desplazaments: {}".format(reference_keys.take(1)))
 
    # Joining querys & keys offsets
    if (DDebug):
        query_sequence.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        keysdespl_rdd.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        print("MultipleQuery:: Querys: {}".format(query_sequence.take(1)))
        print("MultipleQuery:: Query Keys: {}".format(keysdespl_rdd.take(1)))
        print("MultipleQuery:: Matching desplazaments: {}".format(reference_keys.take(1)))
        
    t3 = time()
    query_keys_df = keysdespl_rdd.flatMap(pquerykeys).toDF(["Query", "Key" , "Despl"])
    reference_keys_df = reference_keys.toDF(["Key2" , "Offsets"])
    query_keys_df.groupBy('Key')
    reference_keys_df.groupBy('Key2')
    if (DDebug):
        reference_keys_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        query_keys_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        print("MultipleQuery:: query_keys_df: ")
        query_keys_df.show(10)
        print("MultipleQuery:: reference_keys_df: ")
        reference_keys_df.show(10)
        
    joined_df = query_keys_df.join(reference_keys_df, query_keys_df.Key == reference_keys_df.Key2)
    joined_df = joined_df.drop(joined_df.Key2)
    #joined_df.printSchema()
    t3a = time()
    t_join = t3a-t3

    if (DDebug):
        joined_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        print("######################### Number of partitions joined_df: {}".format(joined_df.rdd.getNumPartitions()))
        print("MultipleQuery:: joined_df:")
        joined_df.show(10)
         
    addDespl_udf = udf(addDespl,  ArrayType(LongType())) 
    joined_df = joined_df.withColumn("Offsets", addDespl_udf('Offsets','Despl'))
    t3b = time()
    t_adddespl = t3b-t3a

    if (DDebug):
        print("MultipleQuery:: joined_df addind despl:")
        joined_df.show()
     
    grouped_df = joined_df.drop(joined_df.Key).groupby('Query').agg(F.collect_list("Offsets")).withColumnRenamed("collect_list(Offsets)", "Offsets") 
    flatten_udf = udf(flatten,  ArrayType(LongType())) 
    grouped_df = grouped_df.withColumn("Offsets", flatten_udf('Offsets'))
    if (DCalculateStageStatistics):  
        grouped_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        print("@@@@@ grouped_df: {}".format(grouped_df.count()))
    t_joining = time()-t3
    print("@@@@@ 3-Joining querys & keys offsets: {}".format(round(t_joining,3)))
    
    if (DDebug):
        grouped_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        print("MultipleQuery:: grouped_df flatten despl:")
        grouped_df.show()
 
    
    
    # 3. Calculate Top-matching offsets
    t4 = time()
    offcount_schema = ArrayType(StructType([
                                    StructField("Offset", IntegerType(), False),
                                    StructField("count", IntegerType(), False)
                                ]))
    countby_udf = udf(countbyfilter,  offcount_schema) 
    counted_df = grouped_df.withColumn("Offsets", countby_udf('Offsets'))
    
    if (DDebug):
        counted_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        print("MultipleQuery:: countbyfilter_udf despl:")
        print(counted_df.take(1))
    
    countby_udf = udf(countbyfilterdespl, ArrayType(LongType())) 
    counted_df = grouped_df.withColumn("Offsets", countby_udf('Offsets'))
    
    if (DDebug):
        print("MultipleQuery:: countbyfilterdespl despl:")
        counted_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        print(counted_df.take(1))
        
    filter_df = counted_df.filter(size('Offsets')>0)
    query_sequence_df = query_sequence.toDF()
    query_offset_df = filter_df.join(query_sequence_df, query_sequence_df._c1 == filter_df.Query)
    query_offset_df = query_offset_df.drop(query_offset_df._c1).drop(query_offset_df._c2).withColumnRenamed("_c0", "QuerySeq")
    if (DCalculateStageStatistics):  
        query_offset_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        if (query_offset_df.rdd==sc.emptyRDD()):
            candidates_offsets = 0
            npartitions = 0
        else:
            candidates_offsets = query_offset_df.select(F.sum(F.size('Offsets'))).collect()[0][0]
            npartitions = query_offset_df.rdd.getNumPartitions()
        if (candidates_offsets is None):
            candidates_offsets = 0
        if (npartitions is None):
            npartitions = 0
        print("@@@@@ candidates_offsets: {}".format(candidates_offsets))
    t_topmat = time()-t4
    print("@@@@@ 4-Calculate Top-matching offsets: {}".format(round(t_topmat,3)))
    
    if (DDebug):
        print("######################### Number of partitions query_offset_df: {}".format(query_offset_df.rdd.getNumPartitions()))
        print("MultipleQuery:: Candidate Offsets: {}".format(candidates_offsets ))
        print("MultipleQuery:: query_offset_df:")
        print(query_offset_df.take(1))

      
    # 4. Make extension in Top-matching zones.
    t5 = time()
    if (False):
        query_offset_df.persist()
        print("@@@@@ Number of partitions Extension (query_offset_df): {}".format(query_offset_df.rdd.getNumPartitions()))
    good_aligments = CalculateAligmentsMultipleQuery(sc, session, query_offset_df, referenceName)
    if (DCalculateStageStatistics):      
        good_aligments.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        if (good_aligments==sc.emptyRDD()):
            n_aligments = 0
        else:
            n_aligments = good_aligments.count()
        print("@@@@@ N good aligments: {}".format(n_aligments))
    n_aligments = good_aligments.count()
    t6 = time()
    t_ext = t6-t5
    print("@@@@@ 5-Aligment extension: {}".format(round(t_ext,3)))
    tt = t6-t0
    print("@@@@@ TOTAL TIME: {}".format(round(tt,3)))


    if (DDebug):
        print("Good Aligments:")       
        print(good_aligments.collect())
        
    if (DShowResult):
        good_aligments.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        map(ShowMultipleQueryAligmentResult,good_aligments.collect()) 
    
    if (DSaveResult):
        global YarnJobId
        ResultFile = DHdfsOutputPath+APP_NAME+"_"+ os.path.splitext(os.path.basename(queryFilename))[0] +"_"+referenceName+"_"+YarnJobId
        print("Writing matching aligments in file {}".format(ResultFile))
        #good_aligments.coalesce(1).saveAsTextFile(ResultFile)
        good_aligments.saveAsTextFile(ResultFile)
        
    session.shutdown()
    cluster.shutdown()
        
    if (DTiming):
        if (DCalculateStatistics):   
            nquerys = 0
            nkeysdespl = 0
            candidates_offsets = 0
            npartitions = 0
            if (not DCalculateStageStatistics):
                query_sequence.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
                keysdespl_rdd.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
                nquerys = 0
                if (query_sequence!=sc.emptyRDD()):
                    nquerys = query_sequence.count()
                if (False):
                    nkeysdespl = 0
                    if (keysdespl_rdd!=sc.emptyRDD()):
                        nkeysdespl = keysdespl_rdd.count()
                    query_offset_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
                    if (query_offset_df.rdd==sc.emptyRDD()):
                        candidates_offsets = 0
                        npartitions = 0
                    else:
                        candidates_offsets = query_offset_df.select(F.sum(F.size('Offsets'))).collect()[0][0]
                        npartitions = query_offset_df.rdd.getNumPartitions()
        print("# Total time required for processing the {} query with {} keys: {} seconds.".format(nquerys, keysdespl_rdd.count(), round(tt,3)))
        executors = sc._conf.get("spark.executor.instances")
        cores = sc.getConf().get("spark.executor.cores")
        memory = sc.getConf().get("spark.executor.memory")
        print("########################### FINAL STATISTICS Multiple QUERY {} ###########################".format(date_time))
        print("# Reference: {}  \tFile: {}   \tQuerys: {}.".format(ReferenceName, queryFilename, nquerys))
        print("# Key Size: {}            \tMethod: {}.".format(KeySize, Method))
        print("# Num Executors: {}      \tExecutors/cores: {}      \tExecutor Mem: {}.".format(executors, cores, memory))
        #print("# Hash Groups Size: {}  \tPartition Size: {}  \tContent Block Size: {}.".format(HashGroupsSize, BlockSize, ContentBlockSize))
        print("# Total Time: {}         \tData Read Time: {}       \tCassandra Read Time: {} .".format(round(tt,3), round(t_read,3), round(t_qcass,3)))
        print("# DF Joining Time: {}    \tTop matching Time: {}   \tAlig. Extension Time: {}.".format(round(t_joining,3), round(t_topmat,3), round(t_ext,3)))
        print("# Num Aligments: {}      \tGood Aligments: {}.".format(candidates_offsets, n_aligments))
        print("############################################################################################")
        #result.persist()
        #print("# Total time required for processing {} keys using {} partitions in {} seconds.".format(result.count(), result.getNumPartitions(), round(tt,3)))
        #print("# Reference data size: {} MBytes.\n".format(round(get_size(ReferenceFilename)/(1024.0*1024.0),3)))
        if (StatisticsFileName):
            write_statistics_multiple_query(StatisticsFileName, queryFilename, ReferenceName, KeySize, date_time, nquerys, candidates_offsets, n_aligments, npartitions, tt, t_read, t_qcass, t_joining, t_topmat, t_ext)

            
    print("Done.")
            
    return good_aligments
    
    
def ShowAligmentResult(aligment):
    global reference_name_bc
    print("Find in "+reference_name_bc.value+": "+str(aligment[0])+' ---> '+str(aligment[1])+", align score: "+str(aligment[2]))
    Display(aligment[3], aligment[4])
    
    
def ShowMultipleQueryAligmentResult(aligment):
    global reference_name_bc
    print("Query " +str(aligment[0])+ " find in "+reference_name_bc.value+": "+str(aligment[1])+' ---> '+str(aligment[2])+", align score: "+str(aligment[3]))
    Display(aligment[4], aligment[5])


#
# Calculate aligment extension.
#
def CalculateAligments(sc, session, querySequence, referenceName, top_matching):
    dfc("CalculateAligments", sc, session, querySequence, referenceName, top_matching)
    
    # Get Content block size  
    querySelect = "SELECT * FROM " + referenceName + "." + DReferenceContentTableName + " WHERE blockid=0"
    for i in range(0,DCassandraRetriesNumber):
        while True:
            try:          
                resultSelect = session.execute(querySelect)
            except:
                # Print exception info.
                print("@@@@@ Capatured Exception")
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                del exc_info
                # Retry operation after a delay.
                sleep(DCassandraRetryTimeout)
                continue
            break
            
    if querySelect is None:
        raise  
    if resultSelect:
        contentBlockSize = resultSelect[0].size
        if (DDebug):
            print("CalculateAligments::Content block size: {}.".format(contentBlockSize))
    else:
        print("ERROR: Refererence contante table {} do not exist.".format(referenceName + "." + DReferenceContentTableName))     
            
    aligments = DistributeAligments(sc, querySequence, referenceName, top_matching, contentBlockSize)
    
    #quality_aligments = aligments.filter(lambda algn: algn[2]>DMinAligmentScore) 
           
    return quality_aligments
    

#
# Distribute the calculation of aligments extension.
#
def DistributeAligments(sc, querySequence, referenceName, offsets_count, contentBlockSize):
    dfc("DistributeAligments", sc, querySequence, referenceName, offsets_count, contentBlockSize)
    
    global query_sequence_bc, content_block_size_bc
    query_sequence_bc = sc.broadcast(querySequence)
    content_block_size_bc = sc.broadcast(contentBlockSize)
    
    if (DDebug):
        print("DistributeAligments: {} {}".format(querySequence, referenceName))
        print("offsets_count: ",)
        print(offsets_count.take(1))
    
    #CassandraAligmentR(offsets_count.collect()[0][0])
    aligments = offsets_count.map(lambda off: CassandraAligmentR(off[0]))
    
    if (DDebug and not aligments.isEmpty()):
        print("ResultAligments: {}".format(aligments.take(1)))
    
    return aligments


def CassandraAligment(record):
    return CassandraAligmentR(record[0])

def CassandraAligmentR(offset):
    global reference_name_bc, query_sequence_bc, content_block_size_bc, key_size_bc
    querySequence = query_sequence_bc.value
    queryLength = len(querySequence)
    referenceName = reference_name_bc.value
    contentBlockSize = content_block_size_bc.value
    keySize  = key_size_bc.value
    
    # calculate reference begin and end
    if DAligmentExtension:
        reference_seq_begin = offset - (queryLength + keySize - 5)
        reference_seq_end = reference_seq_begin + queryLength + keySize
    else:
        reference_seq_begin = offset - (queryLength)
        reference_seq_end = reference_seq_begin + queryLength
        
    reference_seq_begin = offset - (queryLength)
    reference_seq_end = reference_seq_begin + queryLength

#    reference_seq_begin = offset - (queryLength + DAligmentExtensionLength)
#    reference_seq_end = reference_seq_begin + queryLength + DAligmentExtensionLength
        
    
    # Calculate reference content start and end block
    bbegin = reference_seq_begin/contentBlockSize
    bend = reference_seq_end/contentBlockSize
    
    # Get Reference Content Sequence
    end, contentSequence = GetRefereceContentBlocks(referenceName, bbegin, bend)
    if (contentSequence is None):
        return (-1,-1, 0, "Error in GetRefereceContentBlocks","Block Id %d-%d not exist inf table %s" % (bbegin, bend, referenceName))
    #print(contentSequence.first())
    if (DDebug):
        print("Reference Content Sequence {}-{}: {}".format(bbegin, bend, contentSequence))
        
    if (reference_seq_end>end):
        contentSequence = contentSequence + " " * (reference_seq_end-end+1)
    
    # Calculate Alignment
    boffset = reference_seq_begin-(contentBlockSize * bbegin) 
    if (DDebug):
        print("Short Reference Content Sequence {}-{}: {}".format(bbegin, bend, contentSequence[boffset:reference_seq_end]))
    align_seq1, align_seq2, align_score, align_off = DoAligment(querySequence, contentSequence[boffset:reference_seq_end])
    
    if (DDebug):
        print("find in "+referenceName+": "+str(reference_seq_begin+align_off)+' ---> '+str(reference_seq_begin+align_off+queryLength-1)+", align score: "+str(align_score))
        Display(align_seq1, align_seq2)
        
    if (align_score>DMinAligmentScore):
        return (offset+align_off, offset+align_off+queryLength-1, align_score, align_seq1, align_seq2)
    else:
        return sc.emptyRDD()


def CalculateAligmentsMultipleQuery(sc, session, querySequences, referenceName):
    dfc("CalculateAligmentsMultipleQuery", sc, session, querySequences, referenceName)
    
    # Get Content block size
    querySelect = "SELECT * FROM " + referenceName + "." + DReferenceContentTableName + " WHERE blockid=0"
    for i in range(0,DCassandraRetriesNumber):
        while True:
            try:
                resultSelect = session.execute(querySelect)
            except:
                # Print exception info.
                print("@@@@@ Capatured Exception")
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                del exc_info
                # Retry operation after a delay.
                sleep(DCassandraRetryTimeout)
                continue
            break
    if querySelect is None:
        raise  
    
    if resultSelect:
        contentBlockSize = resultSelect[0].size
        if (DDebug):
            print("CalculateAligmentsMultipleQuery::Content block size: {}.".format(contentBlockSize))
    else:
        print("ERROR: Refererence contante table {} do not exist.".format(referenceName + "." + DReferenceContentTableName))
    
    aligments = DistributeAligmentsMultipleQuery(sc, querySequences, referenceName, contentBlockSize)
    if (DDebug):
        aligments.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    
    if not aligments.isEmpty():
        #print("CalculateAligmentsMultipleQuery::aligments: {}".format(aligments.take(10)))
        #quality_aligments = aligments.filter(lambda algn: algn[3]>DMinAligmentScore)    
        return aligments
    else:
        return sc.emptyRDD()
    

def DistributeAligmentsMultipleQuery(sc, querySequences, referenceName, contentBlockSize):
    dfc("DistributeAligmentsMultipleQuery", sc, querySequences, referenceName, contentBlockSize)
    
    global content_block_size_bc
    content_block_size_bc = sc.broadcast(contentBlockSize)
    
    if (DDebug):
        print("DistributeAligmentsMultipleQuery: {} ".format(referenceName))
        print("querySequences: ",)
        print(querySequences.take(1))
    
    #CassandraAligmentR(offsets_count.collect()[0][0])
    #global YarnJobId  
    #querySequences.write.parquet(DHdfsTmpPath+'querySequences_df_'+YarnJobId+'.parquet')
    
    if (DBalanceAligmentsCalculation and querySequences!=sc.emptyRDD()):

        print("###### ORIGINAL RDD ############################################################")
        ShowBalanceStatistics(querySequences.rdd)
        print("###########################################################")

        # Balancing
        balancedQuerySequences = querySequences.rdd.flatMap(BalancingQuerys)

        # Calculate Number of Partitions    
        executors = sc._conf.get("spark.executor.instances")
        cores = sc.getConf().get("spark.executor.cores")
        if executors is None or cores is None:
            total_cores = 1
        else:
            total_cores = int(executors) * int(cores)
        max_partitions = DAligmentMaxNumberStages * total_cores
        balancedQuerySequences.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        naligments = balancedQuerySequences.count()
        if (naligments>max_partitions):
            NumberPartitions = max_partitions
        else:
            NumberPartitions = naligments
            
        if (NumberPartitions==0):
            NumberPartitions = 1

        balancedQuerySequences = balancedQuerySequences.repartition(NumberPartitions)

        print("###### BALANCE RDD {} ############################################################".format(NumberPartitions))
        ShowBalanceStatistics(balancedQuerySequences)
        print("###########################################################")

    else:
        balancedQuerySequences = querySequences.rdd

    aligments = balancedQuerySequences.flatMap(CassandraAligmentMultipleQuery)
    
    if (DDebug and aligments.count()>0):
        print("ResultAligments: {}".format(aligments.take(1)))
    
    return aligments


def CassandraAligmentMultipleQuery(record):
    #if (DDebug and record[0]!=4):
    #    return ([(record[0], -1, -1, 0, "Not Processed","%s" % (record[2]))])
    return CassandraAligmentMultipleQueryR(record[0], record[1], record[2])

def CassandraAligmentMultipleQueryR(queryId, offsetList, querySequence):
    dfc("CassandraAligmentMultipleQueryR", queryId, offsetList, querySequence)
    
    global reference_name_bc, content_block_size_bc, key_size_bc
    referenceName = reference_name_bc.value
    contentBlockSize = content_block_size_bc.value
    keySize  = key_size_bc.value
    
    if (DDebug):
        print("CassandraAligmentMultipleQueryR ({}, {}, {}).".format(queryId, offsetList, querySequence))
    
    for i in range(0,DCassandraRetriesNumber):
        while True:
            try:
                cluster = Cluster(DCassandraNodes)
                session = cluster.connect()  
            except:
                # Print exception info.
                print("@@@@@ Capatured Exception")
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                del exc_info
                # Retry operation after a delay.
                sleep(DCassandraRetryTimeout)
                continue
            break
    if session is None:
        raise  
    
    queryLength = len(querySequence)
    
    aligments = []
    contentSequence = ""
    #print("Offsets {} sort: {}".format(offsetList, offsetList.sort()))
    offsetList.sort()
    for offset in offsetList:
        if (DDebug):
            print("CassandraAligmentMultipleQueryR:: Processing offset {}.".format(offset))
            
        # calculate reference begin and end
        reference_seq_begin = offset - (queryLength + keySize - 5)
        reference_seq_end = reference_seq_begin + queryLength + keySize
        
        
        if DAligmentExtension:
            reference_seq_begin = offset - int(queryLength + math.floor(DAligmentExtensionLength/2.0))
            reference_seq_end = reference_seq_begin + queryLength + int(math.ceil(DAligmentExtensionLength/2.0))
        else:
            reference_seq_begin = offset - (queryLength)
            reference_seq_end = reference_seq_begin + queryLength
        
        # Calculate reference content start and end block
        bbegin = reference_seq_begin/contentBlockSize
        bend = reference_seq_end/contentBlockSize
        
        # Get Reference Content Sequence
        end, contentSequence = GetRefereceContentBlocksCache(session, referenceName, bbegin, bend)
        if (contentSequence is None):
            print("ERROR CassandraAligmentMultipleQueryR: CassandraAligmentMultipleQueryR ({}, {}, {}).".format(queryId, offsetList, querySequence))
            continue
        if (DDebug):
            print("Reference Content Sequence {}-{}: {}".format(bbegin, bend, contentSequence))

        if (reference_seq_end>end):
            contentSequence = contentSequence + " " * (reference_seq_end-end+1)
            
        # Calculate Alignment
        boffset = reference_seq_begin-(contentBlockSize * bbegin) 
        if (DDebug):
            print("Short Reference Content Sequence {}-{}, {}-{}: {}".format(bbegin, bend, reference_seq_begin, reference_seq_end, contentSequence[boffset:reference_seq_end]))
        align_seq1, align_seq2, align_score, align_off = DoAligment(querySequence, contentSequence[boffset:reference_seq_end+1])

        if (DDebug):
            print("find in "+referenceName+": "+str(reference_seq_begin+align_off)+' ---> '+str(reference_seq_begin+align_off+queryLength-1)+", align score: "+str(align_score))
            Display(align_seq1, align_seq2)
            
        if (align_score>DMinAligmentScore):          
            aligments.append((queryId, offset+align_off, offset+align_off+queryLength-1, align_score, align_seq1, align_seq2))
        
    session.shutdown()
    cluster.shutdown()
    
    if (False and len(aligments)==0):
        print("CassandraAligmentMultipleQueryR {} {} {}".format(queryId, offsetList, querySequence))
        aligments.append((queryId, -1, -1, 0, "Error in GetRefereceContentBlocks","Block Id %d-%d not exist inf table %s - offsets %s" % (bbegin, bend, referenceName,offsetList)))

    return(aligments)
    
   
   
def GetRefereceContentBlocks(referenceName, bbegin, bend):
    dfc("GetRefereceContentBlocks", referenceName, bbegin, bend)
    
    cluster = Cluster(DCassandraNodes)
    ses = cluster.connect()  
    
    contentSequence = ""
    blocks_read = 0
    end = 0
    for block in range(bbegin, bend+1):
        # Get Content block
        if (DDebug):
            print("Reading block {} froma {} table.".format(block,referenceName))
        querySelect = "SELECT * FROM " + referenceName + "." + DReferenceContentTableName + " WHERE blockid=%s"
        while True:
            try:
                resultSelect = ses.execute(querySelect, [block])
            except:
                # Print exception info.
                print("@@@@@ Capatured Exception")
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                del exc_info
                # Retry operation after a delay.
                sleep(DCassandraRetryTimeout)
                continue
            break
        
        if resultSelect:
            contentSequence = contentSequence + resultSelect[0].value
            blocks_read +=1
            end = resultSelect[0].offset + resultSelect[0].size
            
    ses.shutdown()
    cluster.shutdown()
    
    if (blocks_read>0):
        return (end, contentSequence)
    else:
        print("ERROR: Refererence Content block id {} do not exist in table {} .".format(end+1,referenceName + "." + DReferenceContentTableName))
        return (0,None)
      
        
        
def static_vars(**kwargs):
    def decorate(func):
        for k in kwargs:
            setattr(func, k, kwargs[k])
        return func
    return decorate

@static_vars(BlockCache = collections.defaultdict(list))
def GetRefereceContentBlocksCache(ses, referenceName, bbegin, bend):
    
    contentSequence = ""
    blocks_read = 0
    end = 0
    for block in range(bbegin, bend+1):
        if block in GetRefereceContentBlocksCache.BlockCache:
            if (DDebug):
                print("HIT block cache {} -> {}.".format(block,GetRefereceContentBlocksCache.BlockCache[block][0]))
            contentSequence = contentSequence + GetRefereceContentBlocksCache.BlockCache[block][1]
            blocks_read +=1
            end = GetRefereceContentBlocksCache.BlockCache[block][0] + len(GetRefereceContentBlocksCache.BlockCache[block][1]) 
        else:
            # Get Content block
            if (DDebug):
                print("MISS block {} Reading froma {} table.".format(block,referenceName))
            querySelect = "SELECT * FROM " + referenceName + "." + DReferenceContentTableName + " WHERE blockid=%s"
            while True:
                try:
                    resultSelect = ses.execute(querySelect, [block])
                except:
                    # Print exception info.
                    print("@@@@@ Capatured Exception")
                    exc_info = sys.exc_info()
                    traceback.print_exception(*exc_info)
                    del exc_info
                    # Retry operation after a delay.
                    sleep(DCassandraRetryTimeout)
                    continue
                break
            
            if resultSelect:
                contentSequence = contentSequence + resultSelect[0].value
                blocks_read +=1
                end = resultSelect[0].offset + resultSelect[0].size
                #Put Block in cache.
                GetRefereceContentBlocksCache.BlockCache[block]=(resultSelect[0].offset, resultSelect[0].value)
                #Pop Blcok
                if block-DBlockCacheSize in GetRefereceContentBlocksCache.BlockCache:
                    GetRefereceContentBlocksCache.BlockCache.pop(block-DBlockCacheSize)
            else:
                print("ERROR2 GetRefereceContentBlocksCache: Refererence Content block id {} ({}-{}) do not exist in table {} .".format(block, bbegin, bend,referenceName + "." + DReferenceContentTableName))
                    
            
    if (blocks_read>0):
        return (end, contentSequence)
    else:
        print("ERROR GetRefereceContentBlocksCache: Refererence Content block id {} ({}-{}) do not exist in table {} .".format(block, bbegin, bend,referenceName + "." + DReferenceContentTableName))
        return (0, None)
    

        
def ProcessCassandraQuery(tuple):

    global reference_name_bc, hash_name_bc
    cluster = Cluster(DCassandraNodes)
    ses = cluster.connect()
            
    res = ProcessCassandraQueryR(tuple[0], tuple[1], ses, reference_name_bc.value, hash_name_bc.value)

    ses.shutdown()
    cluster.shutdown()
    
    return (res)
    
    
def ProcessCassandraQueryR(key, despl, session, referenceName, hashName):
    dfc("ProcessCassandraQueryR", key, despl, session, referenceName, hashName)
        
    global DDebug
    if (DDebug):
        print("Processing Query Record {} with despl {}.".format(key, despl))
   
    if hashName is None:
        querySelect = "SELECT * FROM " + referenceName + "." + DReferenceHashTableName + " WHERE seq=%s"
    else:
        querySelect = "SELECT * FROM " + hashName + "." + DReferenceHashTableName + " WHERE seq=%s"

    if (DDebug):
        print("ProcessCassandraQueryR::Select: {} {}".format(querySelect,key))
        
        
    for i in range(0,DCassandraRetriesNumber):
        while True:
            try:
                resultSelect = session.execute(querySelect, [key] )
            except:
                # Print exception info.
                print("@@@@@ Capatured Exception")
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                del exc_info
                # Retry operation after a delay.
                sleep(DCassandraRetryTimeout)
                continue
            break
    if resultSelect is None:
        raise  

    res = []
    for result_row in resultSelect:
        if (DDebug):
            print("{}.{}->{}".format(result_row.seq, result_row.block, list(map(lambda offset: offset, result_row.value))))
        res.append(map(lambda offset: offset+int(despl), result_row.value))        
        #res.append(map(lambda offset: int(offset)+int(despl), result_row.value))        
    #print(res)
    res = flattened_list = [y for x in res for y in x]           
        
    if (DDebug):
        print("Processing Query Record result: {}.".format(res))
    
    return res
    
      
def ProcessMultipleCassandraQuery(tuple):

    global reference_name_bc
    cluster = Cluster(DCassandraNodes)
    ses = cluster.connect()
            
    res = ProcessMultipleCassandraQueryR(tuple, ses, reference_name_bc.value, hash_name_bc.value)

    ses.shutdown()
    cluster.shutdown()
    
    return (res)
    
    
def ProcessMultipleCassandraQueryR(key, session, referenceName, hashName):
    dfc("ProcessMultipleCassandraQueryR", key, session, referenceName, hashName)
    
    global DDebug
    if (DDebug and False):
        print("Processing Multilple Query Record {}.".format(key))
        
    if hashName is None:
        querySelect = "SELECT * FROM " + referenceName + "." + DReferenceHashTableName + " WHERE seq=%s"
    else:
        querySelect = "SELECT * FROM " + hashName + "." + DReferenceHashTableName + " WHERE seq=%s"
        
    if (DDebug):
        print("ProcessCassandraQueryR::Select: {} {}".format(querySelect,key))
        
    for i in range(0,DCassandraRetriesNumber):
        while True:
            try:
                resultSelect = session.execute(querySelect, [key] )
            except:
                # Print exception info.
                print("@@@@@ Capatured Exception")
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                del exc_info
                # Retry operation after a delay.
                sleep(DCassandraRetryTimeout)
                continue
            break
    if resultSelect is None:
        raise
    
    res = []
    for result_row in resultSelect:
        if (DDebug):
            print("{}.{}->{}".format(result_row.seq, result_row.block, list(map(lambda offset: offset, result_row.value))))
        res.append(map(lambda offset: offset, result_row.value))        
        #res.append(result_row.value)        
        #res.append(map(lambda offset: int(offset)+int(despl), result_row.value))        
    #print(res)
    res = flattened_list = [y for x in res for y in x]           
        
    if (DDebug):
        print("Processing Multilple Query Record result: {}.".format(res))
    
    return ((key, res))



def ProcessMultipleCassandraQueryByPartition(list):

    global reference_name_bc, hash_name_bc
    cluster = Cluster(DCassandraNodes)
    ses = cluster.connect()
            
    res = ProcessMultipleCassandraQueryByPartitionR(list, ses, reference_name_bc.value, hash_name_bc.value)

    ses.shutdown()
    cluster.shutdown()
    
    return (res)



def ProcessMultipleCassandraQueryByPartitionR(list, session, referenceName, hashName):

    global DDebug
    if (DDebug):
        print("Processing Multilple Query Record {}.".format(list))
    
    #print("@@@@@ Processing Multilple Query By Partition size: {}.".format(len(list)))
           
    if (DDebug):
        print(list)
        
    if (DDebug):
        for tuple in list:       
            print("ProcessCassandraQueryByPartition: Key: {}.".format(tuple))
    
    res = map(lambda tuple : ProcessMultipleCassandraQueryP(tuple, session, referenceName, hashName), list)
    #res = flattened_list = [y for x in res for y in x]     

    if (DDebug):
        print("ProcessCassandraQueryByPartition::Result: {}.".format(res))

    return (res)



def ProcessMultipleCassandraQueryP(key, session, referenceName, hashName):
    dfc("ProcessCassandraQueryP", key, session, referenceName, hashName)
        
    global DDebug
    if (DDebug):
        print("Processing Query Record {}.".format(key))
   
    if hashName is None:
        querySelect = "SELECT * FROM " + referenceName + "." + DReferenceHashTableName + " WHERE seq=%s"
    else:
        querySelect = "SELECT * FROM " + hashName + "." + DReferenceHashTableName + " WHERE seq=%s"

    if (DDebug):
        print("ProcessCassandraQueryR::Select: {} {}".format(querySelect,key))
        
        
    for i in range(0,DCassandraRetriesNumber):
        while True:
            try:
                resultSelect = session.execute(querySelect, [key] )
            except:
                # Print exception info.
                print("@@@@@ Capatured Exception")
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                del exc_info
                # Retry operation after a delay.
                sleep(DCassandraRetryTimeout)
                continue
            break
    if resultSelect is None:
        raise  

    res = []
    for result_row in resultSelect:
        if (DDebug):
            print("{}.{}->{}".format(result_row.seq, result_row.block, list(map(lambda offset: offset, result_row.value))))
        res.append(map(lambda offset: offset, result_row.value))   
        #res.append(map(lambda offset: int(offset)+int(despl), result_row.value))        
    #print(res)
    res = flattened_list = [y for x in res for y in x]           
        
    if (DDebug):
        print("Processing Query Record result: {}.".format(res))
    
    return ((key, res))




def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def BalancingQuerys(record):
    id = record[0]
    offsets = record[1]
    sequence = record[2]
    queries = []
    for chunk in chunks(offsets, DMaxOffsetsQuery):
        queries.append((id,chunk,sequence))
    return(queries)


def ShowBalanceStatistics(rdd):
    if (not DDebug):
        return

    print("###### Aligments number distribution among partitions: ")
    partition = 1
    Total = 0
    Max = 0
    for par in rdd.glom().collect():
        alig = 0
        for query in par:
            #print("       ##### {} Query {} -> {} aligments ({}).".format(partition, query[0],len(query[1]),sorted(query[1])))
            alig = alig + len(query[1])
        print("###### Partition {} queries: {} aligments: {}.".format(partition, len(par),  alig))
        partition = partition + 1
        Total = Total + alig
        if (alig>Max):
            Max = alig
    print("###### TOTAL aligments: {}.".format(Total))
    num_part = rdd.getNumPartitions()
    print("###### Number of partitions: {}.".format(num_part))
    if (num_part>=1 and Total>0):    
        ratio = float(Total)/float(num_part)
        print("###### Number of partition: {}  Mean aligms/part: {}  Unbalancing: {}%.".format(num_part, Total/num_part, round(float((Max-ratio)/ratio)*100.0,3)))
    else:
        print("###### Number of partition: {}  Mean aligms/part: {}  Unbalancing: {}%.".format(num_part, 0, 0))




        
# Loads and returns data frame for a table including key space given
def load_and_get_table_df(keys_space_name, table_name):
    table_df = sqlContext.read\
        .format("org.apache.spark.sql.cassandra")\
        .options(table=table_name, keyspace=keys_space_name)\
        .load()
    return table_df
    

## 
## Receives Keys+Desplazament tuples (keysdespl_rdd) and select the keys in cassandra reference table and
## sums the corresponding query Desplazament to the reference Offset
## 
def GetKeysOffsetsInReference(sc, session, referenceName, keysdespl_rdd):
    global DDebug
    if (DDebug):
        print("GetKeysOffsetsInReference")
        
    if (DDebug & False):
        # Show Reference Table
        GenRef = load_and_get_table_df(referenceName, DReferenceContentTableName)
        print("Reference Table:")
        GenRef.show()
        
    if (DDebug):
        print("GetKeysOffsetsInReference::QueryKeys:")
        print(keysdespl_rdd.take(1))
    
    if (DDebug):
        print("GetKeysOffsetsInReference::Process all elements")
    matching_despl_rdd = keysdespl_rdd.flatMap(lambda kv: ProcessCassandraQuery(kv))
          
    if (DDebug & matching_despl_rdd.isEmpty()==False):
        print("GetKeysOffsetsInReference::Result: {}".format(matching_despl_rdd.isEmpty()))
        print(matching_despl_rdd.take(1))
       
    return matching_despl_rdd


## 
## Receives Keys+Desplazament tuples (keysdespl_rdd) and select the keys in cassandra reference table and
## sums the corresponding query Desplazament to the reference Offset
## 
def GetMultipleKeysOffsetsInReference(sc, session, referenceName, keysdespl_rdd):
    dfc("GetMultipleKeysOffsetsInReference", sc, session, referenceName, keysdespl_rdd)
        
    #offsets_rdd = keysdespl_rdd.flatMap(lambda res: off[0] for off in res[1])
    offsets_rdd = keysdespl_rdd.flatMap(lambda res: map(lambda off: off[0], res[1]))
    if (DDebug):
        print("GetKeysOffsetsInReference::Print all {} offsets: {}".format(offsets_rdd.count(), offsets_rdd.sortBy(lambda r:r[0]).collect()))
        
    #print(offsets_rdd.take(100))
    #row = Row("Key") # Or some other column name
    #sc.parallelize(offsets_rdd.collect()).map(row).toDF().write.parquet(DHdfsTmpPath+'offsets_rdd_'+YarnJobId+'.parquet')
    
    offsets_rdd = offsets_rdd.distinct()
    if (DBalanceGetKeysOffsesPartitions):
        global NumberPartitions
        print("@@@@@ Repartition offsets rdd from {} to {}".format(offsets_rdd.getNumPartitions(),NumberPartitions))
        if (NumberPartitions>offsets_rdd.getNumPartitions()):
            offsets_rdd = offsets_rdd.repartition(NumberPartitions)
        else:
            offsets_rdd = offsets_rdd.coalesce(NumberPartitions)
        
    if (DDebug):
        print("GetKeysOffsetsInReference::Print distintct {} offsets: {}".format(offsets_rdd.count(), offsets_rdd.sortBy(lambda r:r[0]).collect()))
               
    if (DDebug):
        print("GetKeysOffsetsInReference::Process all elements")
    if DProcessingByPartitions:
        matching_despl_rdd = offsets_rdd.glom().flatMap(lambda kv: ProcessMultipleCassandraQueryByPartition(kv) if len(kv) > 0 else "")
    else:
        matching_despl_rdd = offsets_rdd.map(lambda kv: ProcessMultipleCassandraQuery(kv))
    
    # Filter emtpy offsets
    matching_despl_rdd =  matching_despl_rdd.filter(lambda off: len(off[1])>0)
          
    if (DDebug and matching_despl_rdd.isEmpty()==False):
        print("GetKeysOffsetsInReference::Result: {}".format(matching_despl_rdd.isEmpty()))
        print(matching_despl_rdd.take(1))
       
    return matching_despl_rdd


def ProcessCassandraQueryByPartition(list):

    global reference_name_bc, DDebug
    
    for i in range(0,DCassandraRetriesNumber):
        while True:
            try:
                cluster = Cluster(DCassandraNodes)
                ses = cluster.connect()
            except:
                # Print exception info.
                print("@@@@@ Capatured Exception")
                exc_info = sys.exc_info()
                traceback.print_exception(*exc_info)
                del exc_info
                # Retry operation after a delay.
                sleep(DCassandraRetryTimeout)
                continue
            break
    if ses is None:
        raise
    
    if (DDebug):
        print(list)
        
    if (DDebug):
        for tuple in list:       
            print("ProcessCassandraQueryByPartition: Key: {}, value: {}.".format(tuple[0], tuple[1]))
    
    res = map(lambda tuple : ProcessCassandraQueryR(tuple[0], tuple[1], ses, reference_name_bc.value), list)
    res = flattened_list = [y for x in res for y in x]     

    if (DDebug):
        print("ProcessCassandraQueryByPartition::Result: {}.".format(res))
        
    ses.shutdown()
    cluster.shutdown()

    return res



## 
## Receives Keys+Desplazament tuples (keysdespl_rdd) and select the keys in cassandra reference table and
## sums the corresponding query Desplazament to the reference Offset
## The procesing is done at high-level one-partition one task, in order to reducer the cassandra connections.

def GetKeysOffsetsInReferenceByPartition(sc, session, referenceName, keysdespl_rdd):
    if (DDebug):
        print("GetKeysOffsetsInReferenceByPartition")
        
    if (DDebug):
        print("GetKeysOffsetsInReferenceByPartition::QueryKeys:")
        print(keysdespl_rdd.take(1))
    
    if (DDebug):
        print("GetKeysOffsetsInReferenceByPartition::Process all elements")
    matching_despl_rdd = keysdespl_rdd.glom().flatMap(lambda kv: ProcessCassandraQueryByPartition(kv) if len(kv) > 0 else "")
          
    if (DDebug & matching_despl_rdd.isEmpty()==False):
        print("GetKeysOffsetsInReferenceByPartition::Result: {}".format(matching_despl_rdd.isEmpty()))
        print(matching_despl_rdd.take(1))
       
    return matching_despl_rdd


    
def CalculateQueryOffset(sc, queryFilename, keySize):
    if (DDebug):
        print("CalculateQueryOffset")

    # Read query file (offset,line) from hdfs
    query_rdd = sc.newAPIHadoopFile(
        queryFilename,
        'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
        'org.apache.hadoop.io.LongWritable',
        'org.apache.hadoop.io.Text',
    )
    if (DDebug):
        print("Query::Input file has {} lines and {} partitions: ".format(query_rdd.count(),query_rdd.getNumPartitions()))
        print("Query::First 10 records: "+format(query_rdd.take(1)))
        #query_rdd.count()
    
    # Calculate Dataframe
    query_df, query_sequence = CreateDataFrame(query_rdd)
    
    if (DDebug):
        print("Query Data Frame: ")
        query_df.show(10)
    
    # Calculate Query length
    queryLength =len(query_sequence)
#    if CreateWindowWithPartitions:
#        row = query_df.rdd.reduce(lambda x, y: x if int(x[3]) > int(y[3]) else y)
#    else:
#        row = query_df.rdd.reduce(lambda x, y: x if int(x[2]) > int(y[2]) else y)
#       
#    queryLength =  int(row['offset']) + int(row['size'])
    global query_length_bc
    query_length_bc = sc.broadcast(queryLength)
    
    # Calculate keys & offsets
    t1 = time()
    query_keys_despl_rdd = query_df.rdd.flatMap(calculateQueryKeysDespl)
            
    if (False and DTiming):
        query_keys_despl_rdd.persist()
        print("Time required for calculate keys in {} seconds.\n".format(round(time() - t1,3)))
        tt = time() - gt0_bc.value
        print("Total time required for processing {} keys using {} partitions in {} seconds.".format(query_keys_despl_rdd.count(), query_keys_despl_rdd.getNumPartitions(), round(tt,3)))
        print("Query data size: {} MBytes.\n".format(round(get_size(queryFilename)/(1024.0*1024.0),3)))

    return query_keys_despl_rdd, query_sequence



def CalculateMultipleQueryOffset(sc, sqlContext, queryFilename, keySize):
    dfc("CalculateMultipleQueryOffset", sc, sqlContext, queryFilename, keySize)
    
    global NumberPartitions    
    # Read querys csv files: (query, id, length)
    query_df = sqlContext.read.option("delimiter", "\\t").csv(queryFilename, inferSchema=True)
    if (DDebug):
        print("######################### Query::Input file has {} sequences and {} partitions: ".format(query_df.count(),query_df.rdd.getNumPartitions()))
        # Calculate Number of Partitions           
    convertedudf = udf(nonasciitoascii)
    query_df = query_df.withColumn("_c0", convertedudf(upper(query_df._c0)))
    query_rdd = query_df.rdd
    
    global KeyMatchingThreshold
    avquerysize = query_df.agg(mean(length(query_df._c0))).first()[0]
    KeyMatchingThreshold = int(avquerysize * DKeyMatchingPercentageThreshold)
    print("@@@@@ Average query size: {}".format(avquerysize))
    print("@@@@@ Key Matching Threshold: {}".format(KeyMatchingThreshold))

    if (DDebug):
        print("######################### Query::Input file has {} sequences and {} partitions: ".format(query_df.count(),query_df.rdd.getNumPartitions()))
        print("Query::First 10 records: "+format(query_df.take(1)))
        query_df.printSchema()
    
    if (False and DBalanceGetKeysOffsesPartitions):
        # Calculate Number of Partitions    
        executors = sc._conf.get("spark.executor.instances")
        cores = sc.getConf().get("spark.executor.cores")
        memory = sc.getConf().get("spark.executor.memory")
        if executors is None or cores is None:
            total_cores = 1
        else:
            total_cores = int(executors) * int(cores)
        max_partitions = DMaxNumberStages * total_cores
        nquerys = query_df.count()
        if (nquerys>max_partitions):
            NumberPartitions = max_partitions
        else:
            NumberPartitions = nquerys

        # Repartition.
        print("######################### Total cores: {}  Max Partition: {}  NQuerys: {}  Calc Partitions: {}  Rdd Partitions: {}".format(total_cores, max_partitions, nquerys, NumberPartitions, query_rdd.getNumPartitions()))

        if (NumberPartitions>query_rdd.getNumPartitions()):
            query_rdd = query_rdd.repartition(NumberPartitions)
        else:
            query_rdd = query_rdd.coalesce(NumberPartitions)
    
        print("######################### Total cores: {}  Max Partition: {}  NQuerys: {}  Calc Partitions: {}  NEW Rdd Partitions: {}".format(total_cores, max_partitions, nquerys, NumberPartitions, query_rdd.getNumPartitions()))
    
    # Calculate keys & offsets
    t1 = time()
    #query_keys_despl_rdd = query_rdd.map(cython_calculateMultipleQueryKeysDespl)
    query_keys_despl_rdd = query_rdd.map(calculateMultipleQueryKeysDespl)
    
    # Repartition.
    if (True or DBalanceGetKeysOffsesPartitions):    
        #query_df.persist()
        #DBalancingQueriesByPartition = 70
        #NumberPartitions = query_df.count()/DBalancingQueriesByPartition
        #if NumberPartitions<1:
        #    NumberPartitions = 1
        if (NumberPartitions>query_rdd.getNumPartitions()):
            query_keys_despl_rdd = query_keys_despl_rdd.repartition(NumberPartitions)
        else:
            query_keys_despl_rdd = query_keys_despl_rdd.coalesce(NumberPartitions)
        print("@@@@@ Number of Partitions: {}".format(NumberPartitions))
            
    if (DTiming and DDebug):
        query_keys_despl_rdd.persist()
        print("######################### Time required for calculate keys in {} seconds.\n".format(round(time() - t1,3)))
        tt = time() - gt0_bc.value
        print("######################### Total time required for processing {} keys using {} partitions in {} seconds.".format(query_keys_despl_rdd.count(), query_keys_despl_rdd.getNumPartitions(), round(tt,3)))
        print("######################### Query data size: {} MBytes.\n".format(round(get_size(queryFilename)/(1024.0*1024.0),3)))

    return query_keys_despl_rdd, query_rdd



def calculateQueryKeysDespl(record):
    return calculateQueryKeysDesplR(record.offset, record.size, record.lines, len(record.lines))
    
    
# Calculate query keys with the following tuples {key,offset}
def calculateQueryKeysDesplR(offset, size, lines, lines_size):
    dfc("calculateQueryKeysDesplR", offset, size, lines, lines_size)
        
    global key_size_bc, query_length_bc
    KeySize  = key_size_bc.value
    QueryLength = query_length_bc.value
    KeysDespl = []
    
    #tg1 = time()
    # Calculate the first and last keys desplazaments.
    first_key = 0
    if (size!=lines_size):    
        # Internal lines.
        last_key = size
    else:
        # Last file line.
        last_key = size - KeySize + 1
        
    offset = QueryLength-(offset+first_key+1)
    for k in range (first_key, last_key):       
        # Add key to python list
        KeysDespl.append((lines[k:k+KeySize],int(offset)))
        offset -= 1
        
    #print("\rProcessing {} keys from offset {} in {} secs".format(len(Keys),offset, round(time() - gt0.value,3), end =" "))
    
    return KeysDespl
  
    
def cython_calculateMultipleQueryKeysDespl(record):
    #print("calculateMultipleQueryKeysDespl {}.".format(record))
    global key_size_bc
    
    cyt_calculateMultipleQueryKeysDesplR = spark_cython('do_query.pyx', 'cython_calculateMultipleQueryKeysDesplR')
        
    #cyt_calculateMultipleQueryKeysDesplR = spark_cython('do_query', 'cython_calculateMultipleQueryKeysDesplR')
    return (record["_c1"], cyt_calculateMultipleQueryKeysDesplR(record._c0.upper().encode('ascii','ignore'), len(record._c0), key_size_bc))


def calculateMultipleQueryKeysDespl(record):
    #print("calculateMultipleQueryKeysDespl {}.".format(record))
    return (record["_c1"], calculateMultipleQueryKeysDesplR(record._c0.upper().encode('ascii','ignore'), len(record._c0)))

       
# Calculate multiple query keys with the following tuples {key,offset}
def calculateMultipleQueryKeysDesplR(query, size):
    dfc("calculateMultipleQueryKeysDesplR", query, size)
        
    global key_size_bc
    KeySize  = key_size_bc.value
    KeysDespl = []
    
    #tg1 = time()
    # Calculate the first and last keys desplazaments.
    first_key = 0
    last_key = size - KeySize + 1
    forbiden_key = "N" * KeySize
        
    offset = size-(first_key+1)
    for k in range (first_key, last_key):       
        # Add key to python list
        if (query[k:k+KeySize]!=forbiden_key):
            KeysDespl.append((query[k:k+KeySize],int(offset)))
        offset -= 1
        
    #print("\rProcessing {} keys from offset {} in {} secs".format(len(Keys),offset, round(time() - gt0.value,3), end =" "))
    
    return KeysDespl
    
    
def CreateDataFrame(reference_rdd):

    # Create DataFrame  
    reference_df = sqlContext.createDataFrame(reference_rdd,["file_offset","line"])

    # Delete first line header if exist
    header = reference_df.first()
    header_size = 0
    if (header.line[0]=='>'):
        header_size = len(header.line)+1
        reference_df = reference_df.filter(reference_df.file_offset!=0)
               
    if (Method==ECreate2LinesData):
        df = Create2LinesDataFrame(reference_df, header_size, BlockSize)
    elif (Method==ECreate1LineDataWithoutDependencies):
        df = Create1LineDataFrameWithoutDependencies(reference_df, header_size, BlockSize)
    elif (Method==ECreateBlocksData):
        df = CreateBlocksDataFrame(reference_df, BlockSize)
    
    # Calculate Query String
    query_string = ''
    query_lines = df.select('line').collect()
    for row in query_lines:
        query_string = query_string + row[0]
        
    df.drop(df.line)
    
    return df, query_string.upper()
    
def nonasciitoascii(unicodestring):
    return unicodestring.encode("ascii","ignore")
    
def Create2LinesDataFrame(df, header_size, blocksize):
    
    if (DDebug):
        print("Method: ECreate2LinesData")
        
    if (CreateWindowWithPartitions and blocksize>0):
        df = df.withColumn("block", (df.file_offset / blocksize).cast("int"))
        my_window = Window.partitionBy("block").orderBy("file_offset")
        if (DDebug):
            print("Spark shuffle partitions:".format(sqlContext.getConf("spark.sql.shuffle.partitions")))
    else: # Without partitions
        my_window = Window.partitionBy().orderBy("file_offset")
    
    convertedudf = udf(nonasciitoascii)
    df1 = df.withColumn("line", convertedudf(upper(df.line)))
    df2 = df1.withColumn("next_line", F.lag(df1.line,-1).over(my_window))
    df3 = df2.withColumn("size", F.length(df2.line)) \
             .withColumn("lines", F.when(F.isnull(df2.next_line), df2.line) \
                                   .otherwise(F.concat(df2.line, df2.next_line))) \
             .withColumn("offset", df2.file_offset-header_size) \
             .drop(df2.next_line).drop(df2.file_offset) 
    #df3.persist()
        
    #print("Query String: ")
    #print("Number of total rows: {} with {} partitions".format(df3.count(),df3.rdd.glom().count()))
    #print("DF3:")
    #df3.show(20) 
    #val rdd = sc.cassandraTable("test", "words")
    if (DDebug):
        df3.persist()
        print("Time required for read and prepare dataframe with {} rows using {} partitions in {} seconds.\n".format(df3.count(), df3.rdd.getNumPartitions(), round(time() - gt0_bc.value,3)))
    #print("DF3:")
    #df3.show(10)
    #print("Time required for read and prepare dataframe in {} seconds.\n".format(round(time() - gt0_bc.value,3)))

    return df3


def Create1LineDataFrameWithoutDependencies(df, header_size, blocksize):
    # Create Blocks of lines to avoid dependencies with the previous line.
    # Ref: https://stackoverflow.com/questions/49468362/combine-text-from-multiple-rows-in-pyspark
  
    if (DDebug):
        print("Method: ECreate1LineDataWithoutDependencies")
    if (CreateWindowWithPartitions and blocksize>0):
        df = df.withColumn("block", (df.file_offset / blocksize).cast("int"))
        my_window = Window.partitionBy("block").orderBy("file_offset")
        if (DDebug):
            print("Spark shuffle partitions:".format(sqlContext.getConf("spark.sql.shuffle.partitions")))
    else: # Without partitions
        my_window = Window.partitionBy().orderBy("file_offset")
    
    convertedudf = udf(nonasciitoascii)
    df = df.withColumn("lines", convertedudf(upper(df.line)))
    df3 = df.withColumn("size", F.length(df.lines)-DKeySize) \
             .withColumn("offset", df.file_offset-header_size) \
             .drop(df.file_offset) 
    #df3.persist()
    #df3.rdd.getNumPartitions() 
    #print("Number of total rows: {} with {} partitions".format(df3.count(),df3.rdd.glom().count()))
    #print("DF3:")
    #df3.show(20) 
    #val rdd = sc.cassandraTable("test", "words")
    #print("Time required for read and prepare dataframe with {} rows using {} partitions in {} seconds.\n".format(df3.count(), df3.rdd.getNumPartitions(), round(time() - gt0_bc.value,3)))
    if (DTiming):
        print("Time required for read and prepare dataframe in {} seconds.\n".format(round(time() - gt0_bc.value,3)))

    return df4

    
def CreateBlocksDataFrame(dfc, blocksize):
    if (DDebug):
        print("Method: ECreateBlocksData")

    #return CreateBlocksDataFrame2(dfc)   
    t1 = time()
    
    global key_size_bc
    keySize = key_size_bc.value
    
    if (CreateWindowWithPartitions and blocksize>0):
        dfc = dfc.withColumn("block", (dfc.offset / blocksize).cast("int"))
        my_window = Window.partitionBy("block").orderBy("offset")
        if (DDebug):
            print("Spark shuffle partitions:".format(sqlContext.getConf("spark.sql.shuffle.partitions")))
    else: # Without partitions
        my_window = Window.partitionBy().orderBy("offset")
       
    #dfc.show(20)
    #my_window = Window.partitionBy().orderBy("offset")     
    #df1 = dfc.select(dfc.value.substr(0,keySize).alias("prefix"))
    df0 = dfc.withColumn("lines", upper(dfc.line))
    df1 = df0.withColumn("prefix",df0.value.substr(0,keySize-1))
    df2 = df1.withColumn("next_line", F.lag(df1.prefix,-1).over(my_window))
    df3 = df2.withColumn("lines", F.when(F.isnull(df2.next_line), df2.value) \
                                   .otherwise(F.concat(df2.value, df2.next_line))) 
    df3 = df3.withColumn("size", F.length(df3.lines)) 
    #df3.sort(col("offset").asc()).show(10)
    df3 = df3.drop("prefix").drop("next_line").drop("value")
    #df3.sort(col("offset").asc()).show(10)
      
    if (DDebug):
        print("Dataframe done with {} rows using {} partitions in {} seconds.\n".format(df3.count(), df3.rdd.getNumPartitions()))
    if (DTiming):
        print("Time required for read and prepare dataframe in {} seconds.\n".format(round(time() - t1,3)))
    
    return df3



def write_statistics_single_query(statisticsFileName, queryFilename, referenceName, keySize, date, nquerys, candidates_offsets, n_aligments, partitions, totalTime, readTime, qcassTime, topmTime, extTime):

    DStatisticsFileHeader = "#Procedure ; Query Method ; Query File Name ; Reference Name ; Date  ; Number of executors ; Cores/Executor ; Memory/Executor ; Total Time (sec) ; Query Read Time (sec) ; Cassandra query Time (sec) ; Top Matching Time (sec) ; Alig. Extension Time (sec) ; Key Size ; Number of Querys ; Candidate Regions ; Number of Good Aligments ; Number of partitions ; Method ;  ; Block Size ; "
    
    executors = sc._conf.get("spark.executor.instances")
    cores = sc.getConf().get("spark.executor.cores")
    memory = sc.getConf().get("spark.executor.memory")
    
    global Method, BlockSize
    if (nquerys is None):
        nquerys = -1
    if (candidates_offsets is None):
        candidates_offsets = -1
    if (n_aligments is None):
        n_aligments = -1
    if (partitions is None):
        partitions = -1
    if (Method is None):
        Method = -1    
    if (BlockSize is None):
        BlockSize = -1   
    
    new_stats = "\"SparkBlast DoQuery \" ; \"Single Query \" ; \"%s\" ; \"%s\" ; \"%s\" ; %s ; %s ; \"%s\" ; %f ; %f ; %f ; %f ; %f ; %d ; %d ; %d ; %d ; %d ; %d ; %d ;" % (queryFilename, referenceName, date, executors, cores, memory, totalTime, readTime, qcassTime, topmTime, extTime, keySize, nquerys, candidates_offsets, n_aligments, partitions, Method, BlockSize)
   
    print("File {} exists? {}".format(DHdfsHomePath+statisticsFileName, check_file(DHdfsHomePath+statisticsFileName)))
 
    # Generate statistics in hdfs using rdd.
    # Read statstics file
    if (not check_file(DHdfsHomePath+statisticsFileName)):
        new_stats_rdd = sc.parallelize([new_stats],1)
    else:
        new_stats_rdd = sc.parallelize([DStatisticsFileHeader, new_stats],1)
    
    global YarnJobId   
    OutputFile = DHdfsTmpPath+"tmp"+"_"+YarnJobId
    new_stats_rdd.saveAsTextFile(OutputFile)
  
    cmd = ['hdfs', 'dfs', '-getmerge',OutputFile+"/part-*", "/tmp/prueba"]
    if (run_cmd(cmd)):
        print("Error getmerge")
    cmd = ['hdfs', 'dfs', '-appendToFile',"/tmp/prueba", DHdfsHomePath+statisticsFileName]
    if (run_cmd(cmd)):
        print("Error appendToFile")
    cmd = ['hdfs', 'dfs', '-rm -R',OutputFile]
    if (run_cmd(cmd)):
        print("Error remove output tmp file")

            
    
def write_statistics_multiple_query(statisticsFileName, queryFilename, referenceName, keySize, date, nquerys, candidates_offsets, n_aligments, partitions, totalTime, readTime, qcassTime, joinTime, topmTime, extTime):

    DStatisticsFileHeader = "#Procedure ; Query Method ; Query File Name ; Reference Name ; Date  ; Number of executors ; Cores/Executor ; Memory/Executor ; Total Time (sec) ; Query Read Time (sec) ; Cassandra query Time (sec) ;  DF Joining Time (sec) ; Top Matching Time (sec) ; Alig. Extension Time (sec) ; Key Size ; Number of Querys ; Candidate Regions ; Number of Good Aligments ; Number of partitions ; Method ; Block Size ; One Query Time (sec) ; One Query Time / Cores (sec) ; "
    
    executors = sc._conf.get("spark.executor.instances")
    cores = sc.getConf().get("spark.executor.cores")
    memory = sc.getConf().get("spark.executor.memory")
    
    global Method, BlockSize
    if (nquerys is None):
        nquerys = -1
    if (candidates_offsets is None):
        candidates_offsets = -1
    if (n_aligments is None):
        n_aligments = -1
    if (partitions is None):
        partitions = -1
    if (Method is None):
        Method = -1    
    if (BlockSize is None):
        BlockSize = -1
    QueryTime=0
    QueryTimeByCore=0
    if (nquerys > 0):
        QueryTime = totalTime/nquerys
        if (executors>0 and cores>0):
            QueryTimeByCore = float(QueryTime) * (float(executors)*float(cores))
        
    new_stats = "\"SparkBlast DoQuery\" ; \"Multiple Query\" ; \"%s\" ; \"%s\" ; \"%s\" ; %s ; %s ; \"%s\" ; %f ; %f ; %f ; %f ; %f ; %f ; %d ; %d ; %d ; %d ; %d ; %d ; %d ; %f ; %f" % (queryFilename, referenceName, date, executors, cores, memory, totalTime, readTime, qcassTime, joinTime, topmTime, extTime, keySize, nquerys, candidates_offsets, n_aligments, partitions, Method, BlockSize, QueryTime, QueryTimeByCore)
   
    print("File {} exists? {}".format(DHdfsHomePath+statisticsFileName, check_file(DHdfsHomePath+statisticsFileName)))
 
    # Generate statistics in hdfs using rdd.
    # Read statstics file
    if (not check_file(DHdfsHomePath+statisticsFileName)):
        new_stats_rdd = sc.parallelize([new_stats],1)
    else:
        new_stats_rdd = sc.parallelize([DStatisticsFileHeader, new_stats],1)
        
    global YarnJobId
    OutputFile = DHdfsTmpPath+"tmp"+"_"+YarnJobId
    new_stats_rdd.saveAsTextFile(OutputFile)
  
    cmd = ['hdfs', 'dfs', '-getmerge',OutputFile+"/part-*", "/tmp/prueba"]
    if (run_cmd(cmd)):
        print("Error getmerge")
    cmd = ['hdfs', 'dfs', '-appendToFile',"/tmp/prueba", DHdfsHomePath+statisticsFileName]
    if (run_cmd(cmd)):
        print("Error appendToFile")  
    cmd = ['hdfs', 'dfs', '-rm -R',OutputFile]
    if (run_cmd(cmd)):
        print("Error remove output tmp file")



        
def spark_cython(module, method):
    def wrapped(*args, **kwargs):
        print("Entered function with: {}".format(args))
        global cython_function_
        try:
            return cython_function_(*args, **kwargs)
        except:
            import pyximport
            #pyximport.install()
            pyximport.install(build_dir=DCythonLibsPath)
            print("Cython compilation complete")
            cython_function_ = getattr(__import__(module), method)
        print("Defined function: {}".format(cython_function_))
        return cython_function_(*args, **kwargs)
    return wrapped


def spark_cython2(*args,**kwargs):
    global cython_function_
    module='do_query'
    method='cython_calculateMultipleQueryKeysDesplR'
    try:
          return cython_function_(*args, **kwargs)
    except:
        import pyximport
        pyximport.install(build_dir=DCythonLibsPath)
        cython_function_ = getattr(__import__(module), method)
    return cython_function_(*args, **kwargs)

    
def dfc(functionName, *args):
    global DDebug
    if (DDebug):
        print("%%%%% [{}]----> {}.".format(time(),functionName))
        #print("[{}]----> {} ({}).".format(time(),functionName,list(args)))
    

    
def run_cmd(args_list):
    print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    proc.communicate()
    print("Return code: {}".format(proc.returncode))
    return proc.returncode
   

def check_file(hdfs_file_path):
    cmd = ['hdfs', 'dfs', '-test', '-e', hdfs_file_path]
    code = run_cmd(cmd)
    return code

  
def remove_file(hdfs_file_path):
    cmd = ['hdfs', 'dfs', '-rm', '-R', hdfs_file_path]
    code = run_cmd(cmd)
    return code


def get_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size



    
def main(sc, sqlContext, queryFilename, referenceName, keySize=DKeySize, DoMultipleQuery=DDoMultipleQuery, HashName=None):
    if (DDebug):
        print("main")
        
    global YarnJobId, cyt_calculateMultipleQueryKeysDesplR
    try:
        a,b,YarnJobId = sc._jsc.sc().applicationId().split('_')
    except: 
        a,YarnJobId = sc._jsc.sc().applicationId().split('-')
        
    sc.addPyFile(DCythonLibsPath+'do_query.pyx')
    
    #import pyximport
    #pyximport.install(build_dir=DCythonLibsPath)
    #import do_query
    #cyt_calculateMultipleQueryKeysDesplR = spark_cython('do_query', 'cython_calculateMultipleQueryKeysDesplR')
    #print("@@@@ cyt_calculateMultipleQueryKeysDesplR result: {}".format( cyt_calculateMultipleQueryKeysDesplR("12345678901234567890",len("12345678901234567890"), 11)))
    
    if (not DoMultipleQuery):
        Query(sc, sqlContext, queryFilename, referenceName, keySize, HashName)
    else:
        MultipleQuery(sc, sqlContext, queryFilename, referenceName, keySize, HashName)

        
        
        
#######################
## Sequences alignments
#######################

def DoAligment(querySequence, referenceSequence):
    ti=time()
    align_seq1,align_seq2,align_score,i_start = Aligment(querySequence, referenceSequence)
    tf=time()
    td=round(tf-ti,3)
    #print("@@@@@@ {} DoAligment({}, {}); {}.".format(td, querySequence, referenceSequence, td))   
    return align_seq1,align_seq2,align_score, i_start

def Aligment(query_seq, candidate_sequence):

    if (False or DDebug):
        print("Alignment")
        print("Candi Seg: {}".format(candidate_sequence))
        print("Query Seg: {}".format(query_seq))
    #candidate_seq_pos = finded_postion - query_seq_length + 11 - 5
    #candidate_seq_length = query_seq_length + 11
    #candidate_sequence = ExtractSeq(chr_index,candidate_seq_pos,candidate_seq_length)
    
    #query_seq = querySequence
    #candidate_sequence = referenceSequence
    #query_seq_length = len(query_seq)
    #candidate_seq_length = query_seq_length + KeySize
    #candidate_seq_pos  = finded_position - query_seq_length + keySize - 5  
    

    if (DAligmentExtension):
        i_start_indexs = []
        for i_start in range(int(math.floor(DAligmentExtensionLength/2.0))+1):
            _,_,score = SMalignment(candidate_sequence[i_start:],query_seq)
            i_start_indexs.append(score)
        #i_start = np.array(i_start_indexs).argmax()
        i_start = i_start_indexs.index(max(i_start_indexs))

        i_end_indexs = []
        for i_end in range(1,int(math.ceil(DAligmentExtensionLength/2.0))+2):
            _,_,score = SMalignment(candidate_sequence[:-i_end],query_seq)
            i_end_indexs.append(score)
        #i_end = np.array(i_end_indexs).argmax()+1
        i_end = i_end_indexs.index(max(i_end_indexs))+1
    else:
        i_start=0
        i_end=1
            
    candidate_sequence = candidate_sequence[i_start:-i_end]
    if (DDebug):
        print("Best aligment {}-{}: {}".format(i_start, i_end, candidate_sequence))
    align_seq1,align_seq2,align_score = SMalignment(candidate_sequence,query_seq)
    
    return align_seq1, align_seq2, align_score, i_start


# compare single base
def SingleBaseCompare(seq1,seq2,i,j):
    if seq1[i] == seq2[j]:
        return 2
    else:
        return -1

#
# Smith-Waterman Alignment
#
def SMalignment(seq1, seq2):
    if (False or DDebug):
        print("SMalignment")
        print("Seg1: {}".format(seq1))
        print("Seg2: {}".format(seq2))
    
    #ti=time()
    m = len(seq1)
    n = len(seq2)
    g = -3
    matrix = []
    for i in range(0, m):
        tmp = []
        for j in range(0, n):
            tmp.append(0)
        matrix.append(tmp)
    for sii in range(0, m):
        matrix[sii][0] = sii*g
    for sjj in range(0, n):
        matrix[0][sjj] = sjj*g
    for siii in range(1, m):
        for sjjj in range(1, n):
            matrix[siii][sjjj] = max(matrix[siii-1][sjjj] + g, matrix[siii - 1][sjjj - 1] + SingleBaseCompare(seq1,seq2,siii, sjjj), matrix[siii][sjjj-1] + g)
    sequ1 = [seq1[m-1]]
    sequ2 = [seq2[n-1]]
    while m > 1 and n > 1:
        if max(matrix[m-1][n-2], matrix[m-2][n-2], matrix[m-2][n-1]) == matrix[m-2][n-2]:
            m -= 1
            n -= 1
            sequ1.append(seq1[m-1])
            sequ2.append(seq2[n-1])
        elif max(matrix[m-1][n-2], matrix[m-2][n-2], matrix[m-2][n-1]) == matrix[m-1][n-2]:
            n -= 1
            sequ1.append('-')
            sequ2.append(seq2[n-1])
        else:
            m -= 1
            sequ1.append(seq1[m-1])
            sequ2.append('-')
    sequ1.reverse()
    sequ2.reverse()
    align_seq1 = ''.join(sequ1)
    align_seq2 = ''.join(sequ2)
    align_score = 0.
    for k in range(0, len(align_seq1)):
        if align_seq1[k] == align_seq2[k]:
            align_score += 1
    align_score = float(align_score)/len(align_seq1)
    #tf=time()
    #td=round(tf-ti,3)
    #if (td>1):
    #    print("@@@@@@ {} SMalignment({}, {}).".format(td, seq1, seq2))
    return align_seq1, align_seq2, align_score


# Display BlAST result
def Display(seque1, seque2):
    le = 40
    while len(seque1)-le >= 0:
        print('sequence1: ',end='')
        for a in list(seque1)[le-40:le]:
            print(a,end='')
        print("\n")
        print('           ',end='')
        for k in range(le-40, le):
            if seque1[k] == seque2[k]:
                print('|',end='')
            else:
                print(' ',end='')
        print("\n")
        print('sequence2: ',end='')
        for b in list(seque2)[le-40:le]:
            print(b,end='')
        print("\n")
        le += 40
    if len(seque1) > le-40:
        print('sequence1: ',end='')
        for a in list(seque1)[le-40:len(seque1)]:
            print(a,end='')
        print("\n")
        print('           ',end='')
        for k in range(le-40, len(seque1)):
            if seque1[k] == seque2[k]:
                print('|',end='')
            else:
                print(' ',end='')
        print("\n")
        print('sequence2: ',end='')
        for b in list(seque2)[le-40:len(seque2)]:
            print(b,end='')
        print("\n")
   

## Testing 

if (DDoTesting):    
       
    # Test 1: Calculate Query's keys & desplazaments (with header line)
    print("Test 1a: Calculate Query's keys & desplazaments (with header line)")
    queryFilename = '../Datasets/References/Query3.txt'
    referenceName = "example2_r100"
    Method = ECreate2LinesData
    #main(sc, sqlContext, queryFilename, referenceName)  
    
    
    # Test 2: Calculate Query's keys & desplazaments (with header line)
    print("Test 2: Calculate Query's keys & desplazaments (with header line)")
    queryFilename = '../Datasets/References/Query_GRCh38.txt'
    referenceName = "grch38_1m"
    Method = ECreate2LinesData
    #main(sc, sqlContext, queryFilename, referenceName.lower())  
    

    # Test 3: Calculate Multiple Querys 
    print("Test 1a: Calculate Multipe Querys")
    queryFilename = '../Datasets/References/MulQuery1.txt'
    referenceName = "example2_r100"
    #MultipleQuery(sc, sqlContext, queryFilename, referenceName)  
    
    # Test 4: Calculate Multiple Querys 
    print("Test 3: Calculate Multipe Querys")
    queryFilename = 'hdfs://babel.udl.cat//user/nando/Datasets/Sequences/GRCh38_latest_genomic_200-400.csv.gz'
    referenceName = "grch38F"
    MultipleQuery(sc, sqlContext, queryFilename, referenceName)  

## End Testing 



if __name__ == "__main__":

    print("__Main__")
    
    ## Process parameters.
    ## SparkBlast_DoQuery <Query_Files> <ReferenceName> [Key_size=11]
    if (len(sys.argv)<2):
        print("Error parametes. Usage: DoQuery [--MQuery] <Query_Files> <ReferenceName> [Key_size=11] [StadisticsFile] [HashName] .\n")
        sys.exit(1)
        
    KeySize = DKeySize
    Method = DDefaultMethod
  
    if (len(sys.argv)>1 and sys.argv[1].upper()=="--MQUERY"):
        DoMultipleQuery = True
        args=2
    else: 
        DoMultipleQuery = False
        args=1
       
    QueryFilename = sys.argv[args]
    ReferenceName = sys.argv[args+1].lower()
    HashName=None
    NumberPartitions = DNumberPartitions
    if (len(sys.argv)>(args+2)):
        KeySize = int(sys.argv[args+2])
    if (len(sys.argv)>args+3):
        StatisticsFileName = sys.argv[args+3]
    if (len(sys.argv)>(args+4)):
        NumberPartitions = int(sys.argv[args+4])
    if (len(sys.argv)>(args+5)):
        HashName = sys.argv[args+5].lower()
    

    ## Configure Spark
    conf = SparkConf().setAppName(APP_NAME+ReferenceName)
    conf.set("spark.sql.shuffle.partitions", NumberPartitions)
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    random.seed()
    
    t0 = time()
    gt0_bc = sc.broadcast(t0)
        
    # Execute Main functionality
    print("{}({}, {}, {}, {}).".format(sys.argv[0], MultipleQuery, QueryFilename, ReferenceName, KeySize, HashName))
    main(sc, sqlContext, QueryFilename, ReferenceName, KeySize, DoMultipleQuery, HashName)
    