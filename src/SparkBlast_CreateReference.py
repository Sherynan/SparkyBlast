# PySpark program to create blast reference hash on cassandra using spark
# Usage: SparkBlast_CreateReference <Reference_Files> [Key_size=11] [ReferenceName=blast] [Method=1] [BlockSize=128K] [ContentBlockSize=1000] [HashGroupSize=10000000] [StatisticsFileName]
   

import pyspark
from pyspark import SparkContext
from pyspark.conf import SparkConf 
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import Row, StringType, StructType, _infer_schema, _has_nulltype, _merge_type, _create_converter
from pyspark.sql.functions import udf, upper, desc, collect_list, udf, min, col, row_number
from pyspark.sql.types import StringType
from cassandra.cluster import Cluster
from enum import Enum
from time import time
import os, shutil, sys, subprocess, math
from subprocess import PIPE, Popen
import random
from datetime import datetime




## Constants
DDoTesting = False
DDebug = False
DTiming = True
APP_NAME = "SparkBlast_CreateHash_"
DKeySize = 21
DReferenceFilename = '../Datasets/References/GRCh38_latest_genomic_10K.fna'
#DReferenceFilename = '../Datasets/References/Example.txt'
DOutputFilename = '../Output/GRCh38_latest_genomic_10K'
#DOutputFilename = "hdfs://babel.udl.cat:8020/user/nando/output/references/GRCh38_latest_genomic/"
DReferenceName = "blast"
DReferenceHashTableName = "hash"
DReferenceContentTableName = "sequences"
DCreateBlocksDataFrame = True
DCreateWindowWithPartitions = False
DNumberPartitions = 1
NumberPartitions = DNumberPartitions
DMaxNumberStages = 4
DMin_Lines_Partition = 200
DPartitionBlockSize = 128  * 1024 
DContentBlockSize = 1000
DHashGroupsSize = 10000000

## Types
# Enum Methods:
ECreate2LinesData = 1
ECreate1LineDataWithoutDependencies = 2
ECreateBlocksData = 3
DDefaultMethod = ECreate2LinesData
   
## Content Creationt Methods
EContentCreationSpark = 1
EContentCreationPython = 2
DDefaultContentCreationMethod = EContentCreationPython
DCalculateStageStatistics = True
DCalculateStatistics = True
    
## Global Variables
Method = DDefaultMethod
CreateWindowWithPartitions = DCreateWindowWithPartitions
BlockSize = DPartitionBlockSize
ContentBlockSize = DContentBlockSize
ContentCreationSpark = DDefaultContentCreationMethod
HashGroupsSize = DHashGroupsSize
StatisticsFileName = None
key_size_bc = 0
hash_groups_size_bc = 0
gt0_bc = time()



## Functions ##
###############

def CreateReference(sc, sqlContext, KeySize, ReferenceFilename, ReferenceName, HashGroupsSize):

    # Broadcast Global variables
    global key_size_bc, gt0_bc, hash_groups_size_bc
    t0 = time()
    gt0_bc = sc.broadcast(t0)
    key_size_bc = sc.broadcast(KeySize)
    hash_groups_size_bc = sc.broadcast(HashGroupsSize)
    
    if (DTiming):
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y %H:%M:%S")
        executors = sc._conf.get("spark.executor.instances")
        cores = sc.getConf().get("spark.executor.cores")
        memory = sc.getConf().get("spark.executor.memory")
        print("++++++++++++ INITIAL STATISTICS {} +++++++++++++".format(date_time))
        print("+ Reference: {}  \tFile: {}.".format(ReferenceName, ReferenceFilename))
        print("+ Key Size: {}   \tMethod: {}.".format(KeySize, Method))
        print("+ Num Executors: {}  \tExecutors/cores: {}  \tExecutor Mem: {}.".format(executors, cores, memory))
        print("+ Hash Groups Size: {}  \tPartition Size: {}  \tContent Block Size: {}.".format(HashGroupsSize, BlockSize, ContentBlockSize))
        print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    # Create Cassandra reference table
    df = CreateCassandraReferenceTables(ReferenceName)

    # Read Reference file (offset,line) from hdfs
    reference_rdd = sc.newAPIHadoopFile(
        ReferenceFilename,
        'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
        'org.apache.hadoop.io.LongWritable',
        'org.apache.hadoop.io.Text',
    )
    if (DCalculateStageStatistics):
        reference_rdd.persist()
        print("@@@@@ Reference size: {}".format(reference_rdd.count()))

    if (DDebug):
        print("Reference:: Input file has {} lines and {} partitions: ".format(reference_rdd.count(),reference_rdd.getNumPartitions()))
        print("Reference::First 10 records: "+format(reference_rdd.take(10)))
        #reference_rdd.count()

    # Create de Reference Content Table
    # Problem: The reference content table has to be created using a standalone python app using linux file.
    #dfc = CreateReferenceContent(reference_rdd, sqlContext, ReferenceFilename, ContentBlockSize, ReferenceName)
   
    t1a = time()
    
    # Create de Reference Index dataframe
    df = CreateDataFrame(reference_rdd)
    if (DCalculateStageStatistics):
        df.persist()
        print("@@@@@ Dataframe size: {}".format(df.count()))

    # Calculate keys
    t1 = time()
    result = df.rdd.flatMap(generateReferenceKeys)
    print(result.take(100))
    ShowBalanceStatistics(result)
    result = result.groupByKey().map(lambda r: (Row(seq=r[0][0],block=r[0][1],value=list(r[1]))))   
    if (DCalculateStageStatistics):
        result.persist()
        print("@@@@@ Keys generated: {}".format(result.count()))

    t2 = time()
        
    if (False):
        result = df.rdd.flatMap(generateReferenceKeys)
        result_bykey = result.groupByKey()
        result = result_bykey.map(lambda r: (Row(seq=r[0][0],block=r[0][1],value=list(r[1]))))   

        if (False):
            result.persist()
            print(result.take(10))        
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print("Spark version: {}.".format(sc.version))
            print("Reference:: Result has {} keys and {} partitions: ".format(result.count(),result.getNumPartitions()))
            new_partitions = int(result.count()/100.0)
            if (new_partitions > 200):
                print("New Partitions: {}.".format(new_partitions))
                result_df= result.toDF().repartition(new_partitions)
            else:
                result_df= result.toDF()
            print("Reference:: Repartitioned Result has {} keys and {} partitions:".format(result_df.count(),result_df.rdd.getNumPartitions()))  
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")


    # Calculate Reference keys and write to cassandra   
    #result.toDF().write.format("org.apache.spark.sql.cassandra").mode('append').options(table=DReferenceHashTableName, keyspace=ReferenceName).save()
    result.toDF().write.format("org.apache.spark.sql.cassandra").mode('overwrite').options(table=DReferenceHashTableName, keyspace=ReferenceName).option("confirm.truncate","true").save()   
    tc = time()

    if (DTiming):
        executors = sc._conf.get("spark.executor.instances")
        cores = sc.getConf().get("spark.executor.cores")
        memory = sc.getConf().get("spark.executor.memory")
        print("########################### FINAL STATISTICS {} ###########################".format(date_time))
        print("# Reference: {}  \tFile: {}.".format(ReferenceName, ReferenceFilename))
        print("# Key Size: {}            \tMethod: {}.".format(KeySize, Method))
        print("# Num Executors: {}      \tExecutors/cores: {}      \tExecutor Mem: {}.".format(executors, cores, memory))
        print("# Hash Groups Size: {}  \tPartition Size: {}  \tContent Block Size: {}.".format(HashGroupsSize, BlockSize, ContentBlockSize))
        print("# Total Time: {}         \tData Read Time: {}   \tData Frame Time: {}.".format(round(tc-t0,3), round(t1a-t0,3), round(t1-t1a,3)))
        print("# Key Calc Time: {}   \tCassandra Write Time: {}.".format(round(t2-t1,3), round(tc-t2,3)))
        print("############################################################################################")
        #result.persist()
        #print("# Total time required for processing {} keys using {} partitions in {} seconds.".format(result.count(), result.getNumPartitions(), round(tt,3)))
        #print("# Reference data size: {} MBytes.\n".format(round(get_size(ReferenceFilename)/(1024.0*1024.0),3)))
        if (StatisticsFileName):
            write_statistics(StatisticsFileName, ReferenceFilename, ReferenceName, KeySize, date_time, result.count(), result.getNumPartitions() , tc-t0, t1a-t0, t1-t1a, t2-t1, tc-t2)

    print("Done.")
    
    return



def ShowBalanceStatistics(rdd):
    if (True or not DDebug):
        return

    print("@@@@@ Offsets number distribution among partitions: ")
    partition = 1
    Total = 0
    Max = 0
    for par in rdd.glom().collect():
        alig = len(par)           
        print("@@@@@ Partition {} queries: {} aligments: {}.".format(partition, len(par),  alig))
        partition = partition + 1
        Total = Total + alig
        if (alig>Max):
            Max = alig
    print("@@@@@ TOTAL offsets: {}.".format(Total))
    num_part = rdd.getNumPartitions()
    print("@@@@@ Number of partitions: {}.".format(num_part))
    if (num_part>=1 and Total>0):    
        ratio = float(Total)/float(num_part)
        print("@@@@@ Number of partition: {}  Mean aligms/part: {}  Unbalancing: {}%.".format(num_part, Total/num_part, round(float((Max-ratio)/ratio)*100.0,3)))
    else:
        print("@@@@@ Number of partition: {}  Mean aligms/part: {}  Unbalancing: {}%.".format(num_part, 0, 0))
        

#
# Create the sequences and hash tables for the Reference
#
def CreateCassandraReferenceTables(ReferenceName):
    #cluster = Cluster(['babel.udl.cat', 'client1.babel', 'client2.babel', 'client3.babel', 'client4.babel',' client5.babel'])
    #cluster = Cluster(['192.168.1.1', '192.168.1.2', '192.168.1.3', '192.168.1.4', '192.168.1.5', '192.168.1.6'])
    cluster = Cluster(['192.168.1.1'])
    session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS "+ ReferenceName +" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };")
    session.execute("DROP TABLE IF EXISTS "+ ReferenceName +"."+ DReferenceHashTableName +";")
    session.execute("CREATE TABLE "+ ReferenceName + "."+ DReferenceHashTableName +" (seq text, block int, value list<bigint>, PRIMARY KEY(seq,block));")
    #session.execute("DROP TABLE IF EXISTS "+ ReferenceName +"."+ DReferenceContentTableName +";")
    #session.execute("CREATE TABLE "+ ReferenceName + "."+ DReferenceContentTableName +" (blockid bigint, offset bigint, size int, value text, PRIMARY KEY(blockID));")
    session.shutdown()
    cluster.shutdown()
       
    if (DDebug):
        print("Cassandra Database Created.")
        
    
#
# Create a table to store the reference content on Cassandra, indexed by the block.    
#
def CreateReferenceContent(reference_rdd, sqlContext, referenceFilename, contblocksize, referenceName): 

    t1 = time()
    
    if (ContentCreationSpark==EContentCreationSpark):
        df = CreateReferenceContentSpark(reference_rdd, sqlContext, contblocksize, referenceName)
    elif (ContentCreationSpark==EContentCreationPython):
        df = CreateReferenceContentPython(sqlContext, referenceFilename, contblocksize, referenceName)
    else:
        print("CreateReferenceContent unknown content creation method {}."+format(ContentCreationSpark))
    
    if (DTiming):
        t2 = time()
        print("Time required for create reference dataframe, with block size {} in {} seconds.\n".format(contblocksize, round(t2 - t1,3)))

    
#
# Create a table to store the reference content on Cassandra, indexed by the block.    
#
def CreateReferenceContentSpark(reference_rdd, sqlContext, contblocksize, referenceName):  
    print("Method: CreateReferenceContent")
    
    t1 = time()
       
    # Create DataFrame  
    reference_df = sqlContext.createDataFrame(reference_rdd,["file_offset","line"])
    
    # Delete first line header if exist
    header = reference_df.first()
    header_size = 0
    if (header.line[0]=='>'):
        header_size = len(header.line)+1
        df = reference_df.filter(reference_df.file_offset!=0)
    else:
        df = reference_df

    if (DDebug & False):
        print("CreateReferenceContent::Show original dataframe:".format(df.show()))

    # Calculate BlockId
    df1 = df.withColumn("blockid", (df.file_offset / contblocksize).cast("int"))
    if (DDebug& False):
        print("CreateReferenceContent::Show distincs groups:".format(df1.show()))
    
    # Concatenate lines to create blocks
    my_window = Window.partitionBy("blockid").orderBy("offset")
    grouped_df1 = df1.groupby('blockid').agg(collect_list("line").alias("value")).drop('line')
    if (DDebug & False):
        print("CreateReferenceContent::Agrupped 1 dataframe:".format(grouped_df1.show(10)))   

    grouped_df2 = df1.groupby('blockid').agg(min("file_offset").alias("offset"))
    if (DDebug & False):
        print("CreateReferenceContent::Agrupped 2 dataframe:".format(grouped_df2.show(10)))    
        
    joined_df = grouped_df1.join(grouped_df2, grouped_df1.blockid==grouped_df2.blockid).drop(grouped_df2.blockid)
    if (DDebug & False):
        print("CreateReferenceContent::Joined dataframe:".format(joined_df.show(10)))   

    concat_list = udf(lambda lst: "".join(lst), StringType())
    cass_reference_df = joined_df.withColumn("value", concat_list(grouped_df1.value)) \
                          .withColumn("size", F.length("value")) \
                          .sort(col("offset").asc())
        
    if (DDebug):
        print("CreateReferenceContent::Agrupped 2 dataframe:".format(cass_reference_df.show(10)))    
        
    t2 = time()
    
    # Write Reference Dataframe to cassandra.   
    cass_reference_df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table=DReferenceContentTableName, keyspace=referenceName).save()

    if (DTiming):
        print("Time required for write cassandra reference table with block size {} in {} seconds.\n".format(contblocksize, round(time() - t2,3)))
        tt = time() - t1
        print("Total time to create reference table: {} seconds.".format(round(tt,3)))
    
    return cass_reference_df 


#
# Create a table to store the reference content on Cassandra, indexed by the block.    
#
def CreateReferenceContentPython(sqlContext, referenceFilename, contblocksize, referenceName):  
    
    if (DDebug):
        print("CreateReferenceContentPython")
    
    # Create Cassandra Table
    obj = SparkBlast_CreateReferenceContent. SparkBlast_CreateReferenceContent(referenceFilename, referenceName, contblocksize)
    obj.CreateReferencoContentTable()
    
    # Create and load RDD from Cassandra Table.
    cass_reference_df = sqlContext.read.format("org.apache.spark.sql.cassandra").\
                                  load(keyspace=referenceName, table=DReferenceContentTableName)
    if (DDebug):    
        print(cass_reference_df)
        cass_reference_df.show(10)
    
    return cass_reference_df

    
#
# Crates dataframe (offset,size,lines) from imput file rdd.
#       
def CreateDataFrame(reference_rdd):

    # Create DataFrame  
    reference_df = sqlContext.createDataFrame(reference_rdd,["file_offset","line"])
    #reference_df = sqlc.createDataFrame(reference_rdd.take(5000),["file_offset","line"])

    # Delete first line header if exist
    header = reference_df.first()
    header_size = 0
    if (header.line[0]=='>'):
        header_size = len(header.line)+1
        df1 = reference_df.filter(reference_df.file_offset!=0)
    else:
        df1 = reference_df
           
    if (Method==ECreate2LinesData):
        df = Create2LinesDataFrame(df1, header_size, BlockSize)
    elif (Method==ECreate1LineDataWithoutDependencies):
        df = Create1LineDataFrameWithoutDependencies(df1, header_size)
    elif (Method==ECreateBlocksData):
        df = CreateBlocksDataFrame(df1, header_size, BlockSize)

    return df
    
#
# Crates 2 lines dataframe from imput file rdd.
#       
def Create2LinesDataFrame(df, header_size, blocksize):
    
    t1 = time()
    
    if (False and CreateWindowWithPartitions and blocksize>0):
        df = df.withColumn("block", (df.file_offset / blocksize).cast("int"))
        my_window = Window.partitionBy("block").orderBy("file_offset")
        print("Spark shuffle partitions:".format(sqlContext.getConf("spark.sql.shuffle.partitions")))
    else: # Without partitions
        my_window = Window.partitionBy().orderBy("file_offset")
          
    df1 = df.withColumn("line", upper(df.line)) \
           .withColumn("id",row_number().over(my_window))  
    df2 = df1.withColumn("next_line", F.lag(df1.line,-1).over(my_window))      
    df3 = df2.withColumn("size", F.length(df2.line)) \
             .withColumn("lines", F.when(F.isnull(df2.next_line), df2.line) \
                                   .otherwise(F.concat(df2.line, df2.next_line))) \
             .withColumn("offset", (df2.file_offset-header_size)-(df2.id-1)) \
             .drop(df2.line).drop(df2.next_line).drop(df2.file_offset).drop(df2.id)   
    
    # Calculate Number of Partitions    
    global NumberPartitions
    executors = sc._conf.get("spark.executor.instances")
    cores = sc.getConf().get("spark.executor.cores")
    if executors is None or cores is None:
        total_cores = 1
    else:
        total_cores = int(executors) * int(cores)
    print("@@@@@ Total cores: {}".format(total_cores))
    max_partitions = DMaxNumberStages * total_cores
    print("@@@@@ Maxpartitions: {}".format(max_partitions))
    lines = df3.count()
    lines_part = lines/max_partitions
    print("@@@@@ Lines: {}  Lines/Part: {}".format(lines,lines_part))
    if (lines_part>DMin_Lines_Partition):
        NumberPartitions = max_partitions
        print("@@@@@ MAXPARTITIONS Number of partitions: {}".format(NumberPartitions))        
    else:
        NumberPartitions = lines/DMin_Lines_Partition
    if (NumberPartitions==0):
        NumberPartitions=1
    print("@@@@@ Number of partitions: {}".format(NumberPartitions))
    df3 = df3.repartition(NumberPartitions)
    
    if (DTiming):
        #print("Time required for read and prepare dataframe with {} rows using {} partitions in {} seconds.\n".format(df3.count(), df3.rdd.getNumPartitions(), round(time() - gt0_bc.value,3)))
        print("Time required for read and prepare dataframe in {} seconds.\n".format(round(time() - t1,3)))

    return df3


#
# Crates 1 lines dataframe from imput file rdd.
#       
def Create1LineDataFrameWithoutDependencies(df, header_size):
    # Create Blocks of lines to avoid dependencies with the previous line.
    # Ref: https://stackoverflow.com/questions/49468362/combine-text-from-multiple-rows-in-pyspark
  
    my_window = Window.partitionBy().orderBy("file_offset")
    df1 = df.withColumn("lines", upper(df.line)) \
           .withColumn("id",row_number().over(my_window))
    df3 = df.withColumn("size", F.length(df.lines)-DKeySize) \
             .withColumn("offset", (df.file_offset-header_size)-(df2.id-1)) \
             .drop(df.line).drop(df.file_offset).drop(df.id)    
    #df3.persist()
    #df3.rdd.getNumPartitions() 
    #print("Number of total rows: {} with {} partitions".format(df3.count(),df3.rdd.glom().count()))
    #print("DF3:")
    #df3.show(20) 
    #val rdd = sc.cassandraTable("test", "words")
    #print("Time required for read and prepare dataframe with {} rows using {} partitions in {} seconds.\n".format(df3.count(), df3.rdd.getNumPartitions(), round(time() - gt0_bc.value,3)))
    
    if (DTiming):
        print("Time required for read and prepare dataframe in {} seconds.\n".format(round(time() - gt0_bc.value,3)))

    return df3
      
#
# Crates blocks of lines dataframe from imput file rdd.
#        
def CreateBlocksDataFrame(dfc, header_size, blocksize):
    if (DDebug):
        print("Method: ECreateBlocksData")
        dfc.show(10)

    #return CreateBlocksDataFrame2(dfc)   
    t1 = time()
    
    global key_size_bc
    keySize = key_size_bc.value
    
    if (False and CreateWindowWithPartitions and blocksize>0):
        dfc = dfc.withColumn("block", (dfc.offset / blocksize).cast("int"))
        my_window = Window.partitionBy("block").orderBy("file_offset")
        if (DDebug):
            print("Spark shuffle partitions:".format(sqlContext.getConf("spark.sql.shuffle.partitions")))
    else: # Without partitions
        my_window = Window.partitionBy().orderBy("file_offset")
       
    #dfc.show(20)
    #my_window = Window.partitionBy().orderBy("offset")     
    #df1 = dfc.select(dfc.value.substr(0,keySize).alias("prefix"))
    df0 = dfc.withColumn("lines", upper(dfc.line))
    df1 = df0.withColumn("prefix",df0.lines.substr(0,keySize-1))
    df2 = df1.withColumn("next_line", F.lag(df1.prefix,-1).over(my_window))
    df3 = df2.withColumn("lines", F.when(F.isnull(df2.next_line), df2.lines) \
                                   .otherwise(F.concat(df2.lines, df2.next_line))) \
             .withColumn("offset", df2.file_offset-header_size) 
    df3 = df3.withColumn("size", F.length(df3.lines)) 
    #df3.sort(col("offset").asc()).show(10)
    df3 = df3.drop("prefix").drop("next_line").drop("line").drop(df3.file_offset) 
    #df3.sort(col("offset").asc()).show(10)
      
    if (DTiming):
        print("Time required for read and prepare dataframe in {} seconds.\n".format(round(time() - t1,3)))
    
    return df3


#
# Crates blocks of lines dataframe from imput file rdd (deprecated).
# 
def CreateBlocksDataFrame2(dfc):
    if (DDebug):
        print("Method: ECreateBlocksData2")
 
    t1 = time()
    
    df3 = dfc.withColumn("lines",dfc.value).drop("value")
    
    if (DDebug):
        print("Dataframe done with {} rows using {} partitions in {} seconds.\n".format(df3.count(), df3.rdd.getNumPartitions()))
    if (DTiming):
        print("Time required for read and prepare dataframe in {} seconds.\n".format(round(time() - t1,3)))
    
    return df3
 


def AddCassandraKey(key, offset):
    listText = str(offset)
    print(key, listText)
    cluster = Cluster(['192.168.1.1', '192.168.1.2', '192.168.1.3', '192.168.1.4', '192.168.1.5', '192.168.1.6'])
    session = cluster.connect()
    session.execute("update blast.sequences set value = value + [%s] where seq =%s;", (listText,key))
    #rddUpdateSeq = sc.parallelize([Row(seq=key,value=listText)])
    #rddUpdateSeq.toDF().write\
    #.format("org.apache.spark.sql.cassandra")\
   # .mode('append')\
   # .options(table="sequences", keyspace="blast")\
   # .save()
    session.shutdown()
    cluster.shutdown()
    return


def generateReferenceKeys(record):
    return generateReferenceKeysR(record.offset, record.size, record.lines, len(record.lines))
    
    
# Generate reference keys with the following tuples {key,offset}
# Does not store the key extension 
def generateReferenceKeysR(offset, size, lines, lines_size):
 
    global key_size_bc, hash_groups_size_bc
    KeySize  = key_size_bc.value
    HashGroupsSize = hash_groups_size_bc.value
    Keys = [] 

    #tg1 = time()
    # Calculate the first and last keys desplazaments.
    first_key = 0
    if (size!=lines_size):    
        # Internal lines.
        last_key = size
    else:
        # Last file line.
        last_key = size - KeySize + 1
    
    forbiden_key = "N" * KeySize
   
    for k in range (first_key, last_key):
        # Add key to casandra
        #AddCassandraKey(lines[k:k+Key_size], offset+k)
        
        # Add key to python list
        #Keys.append({'Key':lines[k:k+Key_size],'Offset':offset+k})
        #Keys.append((lines[k:k+Key_size],str(offset+k)))
        #Keys.append({'seq':lines[k:k+KeySize],'value':[offset+k]})
        #Keys.append(Row(seq=lines[k:k+KeySize],value=[offset+k]))
        if (lines[k:k+KeySize]!=forbiden_key):
            if (offset+k)<0:
                print("ERROR::generateReferenceKeysR ({}, {}, {}, {})->{}".format(offset, size, lines, lines_size, offset+k))
                error
            if (False and lines[k:k+KeySize]=='GAATGCTGTGT'):
                print("##### DEBUG::generateReferenceKeysR GAATGCTGTGT ({}, {}, {}, {})->{}".format(offset, size, lines, lines_size, offset+k))
            
            Keys.append(((lines[k:k+KeySize],(offset+k)/HashGroupsSize),offset+k))
        
    #print("\rProcessing {} keys from offset {} in {} secs".format(len(Keys),offset, round(time() - gt0_bc.value,3), end =" "))
    
    return Keys


def write_statistics(statisticsFileName, referenceFilename, referenceName, keySize, date, keysNumber, partitions, totalTime, readTime, dfTime, keyTime, casTime):

    DStatisticsFileHeader = "#Procedure; Reference File Name ; Reference Name ; Date  ; Number of executors ; Cores/Executor ; Memory/Executor ; Total Time (sec) ; Data Read Time (sec) ; Data Frame Time (sec) ; Key Calc Time (sec) ; Cassandra Write Time (sec) ; Key Size; Number of Keys ; Number of partitions ; Method ; Hash Group Size ; Hash Groups Size ; Block Size ; Content Block Size ; "
    DHdfsHomePath = "hdfs://babel.udl.cat/user/nando/"
    DHdfsTmpPath = DHdfsHomePath + "Tmp/"

    executors = sc._conf.get("spark.executor.instances")
    cores = sc.getConf().get("spark.executor.cores")
    memory = sc.getConf().get("spark.executor.memory")
    
    new_stats = "\"SparkBlast Create Hash\" ; \"%s\" ; \"%s\" ; \"%s\" ; %s ; %s ; \"%s\" ; %f ; %f ; %f ; %f ; %f ; %d ; %d ; %d ; %d ; %d ; %d ; %d ; " % (referenceFilename, referenceName, date, executors, cores, memory, totalTime, readTime, dfTime, keyTime, casTime, keySize, keysNumber, partitions, Method, HashGroupsSize, BlockSize, ContentBlockSize)

        
    if (False):
        # Generate statistics in linux file.
        if os.path.isfile(statisticsFileName):
            statisticsFile = open(statisticsFileName,"a")
        else:
            statisticsFile = open(statisticsFileName,"w")
            statisticsFile.write(DStatisticsFileHeader)    

        #new_stats = "SparkBlast Create Hash ; " + referenceFilename + " ; " + referenceName + " ; " + str(totalTime) + " ; " + str(readTime) + " ; "+ str(dfTime) + " ; " + str(keyTime) + " ; " + str(casTime) + " ; " + str(keySize) + " ; " + str(keysNumber) + " ; " + str(partitions) + " ; " + str(Method) + " ; " + str(HashGroupsSize) + " ; " + str(BlockSize) + " ; " + str(ContentBlockSize) + " ;")
        statisticsFile.write(new_stats + "\n")
        statisticsFile.close()

        # Copy statistics file into hdfs
        #cmd = ['hdfs', 'dfs', '-put', statisticsFileName, DHdfsResultsPath]
        #run_cmd(cmd)

        print(new_stats)
        executors = sc._conf.get("spark.executor.instances")
        cores = sc.getConf().get("spark.executor.cores")
        memory = sc.getConf().get("spark.executor.memory")
        print("Print number of executors: {} \tExecutor cores: {} \tExecutors memory".format(executors, cores, memory))

    
    print("File {} exists? {}".format(DHdfsHomePath+statisticsFileName, check_file(DHdfsHomePath+statisticsFileName)))
 
    # Generate statistics in hdfs using rdd.
    # Read statstics file
    if (not check_file(DHdfsHomePath+statisticsFileName)):
        new_stats_rdd = sc.parallelize([new_stats],1)
    else:
        new_stats_rdd = sc.parallelize([DStatisticsFileHeader, new_stats],1)
        
    OutputFile = DHdfsTmpPath+"tmp"+str(random.randint(1, 10000))
    remove_file(OutputFile)
    new_stats_rdd.saveAsTextFile(OutputFile)
  
    cmd = ['hdfs', 'dfs', '-getmerge',OutputFile+"/part-*", "/tmp/prueba"]
    if (run_cmd(cmd)):
        print("Error getmerge")
    cmd = ['hdfs', 'dfs', '-appendToFile',"/tmp/prueba", DHdfsHomePath+statisticsFileName]
    if (run_cmd(cmd)):
        print("Error appendToFile")
    
    remove_file(OutputFile)         


def TestCreateReference(sc, KeySize, ReferenceName):
    
    # Broadcast Global variables
    global key_size_bc, gt0_bc
    t0 = time()
    gt0_bc = sc.broadcast(t0)
    key_size_bc = sc.broadcast(KeySize)
    
    # Test input data
    line1='123456789012345678901234567890'
    line2='abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz'
    line3='ZYXWVUTSRQPONMLKJIHGFEDCBAZYXWVUTSRQPONMLKJIHGFEDCBA'
    line4='098765432109876543210987654321'

    # Processing the first line:
    offset = 0
    despl = 0
    size = len(line1)
    lines = line1+line2
    lines_size = len(lines)
    keys1 = generateReferenceKeysR(offset, size, lines, lines_size)
#    keys1_sorted = sorted(keys1, key=lambda d: d['Offset']) 
    print("Keys first line:"+format(keys1))

    # Processing the second line:
    offset = len(line1)
    despl = len(line1)
    size = len(line2)
    lines = line1+line2+line3
    lines_size = len(lines)
    keys2 = generateReferenceKeysR(offset, size, lines, lines_size)
    print("Keys second line:"+format(keys2))

    # Processing the third line:
    offset += len(line2)
    despl = len(line2)
    size = len(line3)
    lines = line2+line3+line4
    lines_size = len(lines)
    keys3 = generateReferenceKeysR(offset, size, lines, lines_size)
    print("Keys third line:"+format(keys3))

    # Processing the last line (line4):
    offset += len(line3)
    despl = len(line3)
    size = len(line4)
    lines = line3+line4
    lines_size = len(lines)
    keys4 = generateReferenceKeysR(offset, size, lines, lines_size)
    print("Keys fourth line:"+format(keys4))
    print("Number of keys:"+format(len(line1)+len(line2)+len(line3)+len(line4)-KeySize+1))




def array_to_string(my_list):
    return '[' + ','.join([str(elem) for elem in my_list]) + ']'


def CreateReferenceToFile(sc, sqlContext, KeySize, ReferenceFilename, OutputFilename):

    print("CreateReferenceToFile: {}, {}, {}.".format(KeySize, ReferenceFilename, OutputFilename))
    
    print round(get_size()/(1024.0*1024.0),3)

    if os.path.exists(OutputFilename): 
        shutil.rmtree(OutputFilename)
        os.system("hdfs dfs -rm -R "+OutputFilename)
        
    # Broadcast Global variables
    global key_size_bc, gt0_bc
    t0 = time()
    gt0_bc = sc.broadcast(t0)
    key_size_bc = sc.broadcast(KeySize)

    # Register UDF function
    #printRecord_udf = udf(printRecord)
    #decOffset_udf = udf(decOffset, LongType())
        

    # Read Reference file (offset,line) from linux
    '''
    reference_rdd = sc.newAPIHadoopFile(
        'Datasets/References/GRCh38_latest_genomic.fna.gz',
        'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
        'org.apache.hadoop.io.LongWritable',
        'org.apache.hadoop.io.Text',
    )
    # Read Reference file (offset,line) from hdfs
    reference_rdd = sc.newAPIHadoopFile(
        '/user/nando/Datasets/References/GRCh38_latest_genomic.fna.gz',
        'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
        'org.apache.hadoop.io.LongWritable',
        'org.apache.hadoop.io.Text',
    )
    '''
    # Read Reference file (offset,line) from hdfs
    reference_rdd = sc.newAPIHadoopFile(
        ReferenceFilename,
        'org.apache.hadoop.mapreduce.lib.input.TextInputFormat',
        'org.apache.hadoop.io.LongWritable',
        'org.apache.hadoop.io.Text',
    )

    #reference_rdd.cache()
    print("Reference::Number of partitions: "+format(reference_rdd.getNumPartitions()))
    #print("Reference::First 10 records: "+format(reference_rdd.take(10)))
    #reference_rdd.count()

    reference_df = sqlContext.createDataFrame(reference_rdd,["file_offset","line"])
    #reference_df = sqlContext.createDataFrame(reference_rdd.take(1000),["file_offset","line"])
    header = reference_df.first()
    header_size = len(header.line)+1
    df1 = reference_df.filter(reference_df.file_offset!=0)

    my_window = Window.partitionBy().orderBy("file_offset")
    df1 = df1.withColumn("line", upper(df1.line))
    df2a = df1.withColumn("next_line", F.lag(df1.line,-1).over(my_window))
    df3 = df2a.withColumn("size", F.length(df2a.line)) \
           .withColumn("lines", F.when(F.isnull(df2a.next_line), df2a.line)
                                 .otherwise(F.concat(df2a.line, df2a.next_line))) \
           .withColumn("offset", df2a.file_offset-header_size) \
           .drop(df2a.line).drop(df2a.next_line).drop(df2a.file_offset)
    #df3.persist()
    #print("Number of total rows:"+format(df3.count()))
    #print("DF3:")
    #df3.show(20)

    #tt = time() - gt0_bc.value
    print("Time required for read and prepare dataframe with {} rows using {} partitions in {} seconds.\n".format(df3.count(), df3.rdd.getNumPartitions(), round(time() - gt0_bc.value,3)))
    t1 = time()

    #my_window = Window.partitionBy().orderBy("offset")
    #df2b = reference_df.withColumn("next_offset", F.lag(reference_df.offset,-1).over(my_window))
    #df3b = df2.withColumn("two_lines", F.when(F.isnull(F.concat(df2.prev_line, df2.line)), df2.line)
    #                                   .otherwise(F.concat(df2.prev_line, df2.line)))
    #print("DF2B:")
    #df2b.show()

    #df3 = df3.filter(df3.offset>20000)

    #df4.foreach(printRecord)

    #df5 = df4.select("offset", "two_lines", printRecord3("offset", "two_lines"))
    #print("DF5:")
    #df5.show()

    # Calculate keys
    result = df3.rdd.map(generateReferenceKeys)
    #print("Number of keys generated: "+format(result.count()))
    #tt = time() - t1
    print("Time required for calculate {} keys using {} partitions in {} seconds.\n".format(result.count(), df3.rdd.getNumPartitions(), round(time() - t1,3)))
    t2 = time()

    # Calculate results dataframe
    #df5 = result.flatMap(lambda list: map(lambda key: (key['Offset'], key['Key'], key['Extension']), list )).toDF(("Offset", "Key", "Extension"))
    #df5 = result.flatMap(lambda list: map(lambda key: (key['Offset'], key['Key']), list)).toDF(("Offset", "Key"))
    df5 = result.flatMap(lambda list: map(lambda key: (key['seq'], key['value']), list)).reduceByKey(lambda x,y: x + y).toDF(("Key", "Offset"))
    df5.persist()
    df5.show(10)
    
    # Write results as linux CSV file
    # Flattening Offset array to be writed as csv
    array_to_string_udf = udf(array_to_string,StringType())
    df5 = df5.withColumn('Offset',array_to_string_udf(df5["Offset"])) 
    # Write results as hdfs CSV file
    df5.write.csv(path=OutputFilename, sep="\t")

    tt = time() - t2
    print("Time required for write {} keys using {} partitions in {} seconds.\n".format(df5.count(), df3.rdd.getNumPartitions(), round(tt,3)))
    #df3.unpersist()
    #df5.unpersist()

    tt = time() - gt0_bc.value
    print("Total time required for processing {} keys using {} partitions in {} seconds.".format(df5.count(), df3.rdd.getNumPartitions(), round(tt,3)))
    print("Reference data size: {} MBytes.\n".format(round(get_size(OutputFilename)/(1024.0*1024.0),3)))
    print("Done.")



def run_cmd(args_list):
    #print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    proc.communicate()
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


def main(sc, sqlContext, keySize=DKeySize, referenceFilename=DReferenceFilename, referenceName="Sequences", hashGroupsSize=DHashGroupsSize):

    print("main")
    CreateReference(sc, sqlContext, keySize, referenceFilename, referenceName, hashGroupsSize)


## Testing 

if (DDoTesting):
    keySize = DKeySize
    # Test 1
    #print("Testing: {}, {}, {}.".format(filename, keySize, referenceName))
    #TestCreateReferenceIndex(sc, keySize)

    # Test 2: Generate reference to file
    inputfile = DReferenceFilename
    outputfile = DOutputFilename
    #CreateReferenceIndexToFile(sc, sqlContext, keySize, inputfile, outputfile)

    
    # Test 3: Generate reference in Cassandra
    referenceFilename = '../Datasets/References/Example.txt'
    referenceName = str.lower("Example")
    Method = ECreateBlocksData 
    ContentBlockSize = 1000
    #main(sc, sqlContext, keySize, referenceFilename, referenceName)
    
    
    # Test 4a: Generate reference in Cassandra
    referenceFilename = '../Datasets/References/GRCh38_latest_genomic_5P.fna'
    referenceName = str.lower("GRCh38_5P_3")
    #referenceFilename = '../Datasets/References/Example.txt'
    #referenceName = str.lower("Example")
    Method = ECreate2LinesData 
    ContentBlockSize = 100
    #main(sc, sqlContext, keySize, referenceFilename, referenceName)
    
    # Test 4b: Generate reference in Cassandra
    referenceFilename = '../Datasets/References/GRCh38_latest_genomic_10K.fna'
    referenceName = str.lower("GRCh38_latest_genomic_10K")
    #referenceFilename = '../Datasets/References/Example.txt'
    #referenceName = str.lower("Example")
    Method = ECreateBlocksData 
    ContentBlockSize = 100
    #main(sc, sqlContext, keySize, referenceFilename, referenceName)
    
    # Test 4c: Generate reference in Cassandra
    referenceFilename = '../Datasets/References/GRCh38_latest_genomic_10K.fna'
    referenceName = str.lower("GRCh38_latest_genomic_10K")
    #referenceFilename = '../Datasets/References/Example.txt'
    #referenceName = str.lower("Example")
    Method = ECreateBlocksData 
    ContentBlockSize = 1000
    #main(sc, sqlContext, keySize, referenceFilename, referenceName)

    # Test 4c: Generate reference in Cassandra
    referenceFilename = '../Datasets/References/GRCh38_latest_genomic_10K.fna'
    referenceName = str.lower("GRCh38_latest_genomic_10K")
    #referenceFilename = '../Datasets/References/Example.txt'
    #referenceName = str.lower("Example")
    Method = ECreateBlocksData 
    ContentBlockSize = 10000
    #main(sc, sqlContext, keySize, referenceFilename, referenceName)
    
    error

## End Testing 

    

if __name__ == "__main__":

    ## Process parameters. (https://docs.python.org/2/library/argparse.html)
    ## SparkBlast_CreateReference <Reference_Files> [Key_size=11] [ReferenceName] [Method] 
    ## [PartitionSize] [ContentBlockSize]
    if (len(sys.argv)<2):
        print("Error parametes. Usage: SparkBlast_CreateReference <Reference_Files> [Key_size={}] [ReferenceName] [Method={}]  [BlockSize={}] [ContentBlockSize={}] [HashGroupsSize={}] [StatisticsFileName].\n".format(DKeySize, DDefaultMethod, DPartitionBlockSize, DContentBlockSize, DHashGroupsSize))
        sys.exit(1)

    ReferenceFilename = sys.argv[1]
    KeySize = DKeySize
    Method = DDefaultMethod
    BlockSize = DPartitionBlockSize
    ContentBlockSize = DContentBlockSize
    HashGroupsSize = DHashGroupsSize
    if (len(sys.argv)>2):
        KeySize = int(sys.argv[2])
    if (len(sys.argv)>3):
        ReferenceName = (sys.argv[3]).lower()
    if (len(sys.argv)>4):
        Method = int(sys.argv[4])
    if (len(sys.argv)>5):
        CreateWindowWithPartitions = False
        BlockSize = int(sys.argv[5])
    if (len(sys.argv)>6):
        ContentBlockSize = int(sys.argv[6])
    if (len(sys.argv)>7):
        HashGroupsSize = int(sys.argv[7])   
    if (len(sys.argv)>8):
        StatisticsFileName = sys.argv[8]

    ## Configure Spark
    conf = SparkConf().setAppName(APP_NAME+ReferenceName)
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    random.seed()
    
    t0 = time()
    gt0_bc = sc.broadcast(t0)
        
    # Execute Main functionality
    print("{}({}, {}, {}, {}, {}, {}, {}).".format(sys.argv[0],ReferenceFilename, KeySize, ReferenceName, Method,  BlockSize, ContentBlockSize, HashGroupsSize))
    main(sc, sqlContext, KeySize, ReferenceFilename, ReferenceName, HashGroupsSize)
