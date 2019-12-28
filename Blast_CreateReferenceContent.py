#!/usr/bin/env python

# Python program to create blast reference content in blocks on cassandra without using spark
# Usage: Blast_CreateReferenceContent <Reference_Files> [ReferenceName=ReferenceFileName] [ContentBlockSize=1000]


import os, sys, logging
import gzip
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement


DDoTesting = False
DDebug = False
DReferenceHashTableName = "hash"
DReferenceContentTableName = "sequences"
DContentBlockSize = 1000
DFutureCheck = True
read_chrs=0
read_data=''

class SparkBlast_CreateReferenceContent:

    def __new__(self):
        self.cluster = None
        self.session = None
        self.keyspace = None
        self.log = None
        self.future = None
        self.ReferenceFilename = None
        self.ReferenceName = None
        self.BlockSize = 100

    # parameterized constructor 
    def __init__(self, referenceFilename, referenceName, contentBlockSize): 
        self.ReferenceFilename = referenceFilename
        self.ReferenceName = referenceName
        self.BlockSize = contentBlockSize


    def __del__(self):
        self.session.shutdown()
        self.cluster.shutdown()

    def CreateCassandraSession(self):
        self.cluster = Cluster(['192.168.1.1', '192.168.1.2', '192.168.1.3', '192.168.1.4', '192.168.1.5', '192.168.1.6'])
        self.cluster = Cluster(['192.168.1.1'])
        self.session = self.cluster.connect()

    def GetCassandraSession(self):
        return self.session

    # How about Adding some log info to see what went wrong
    def SetLogger(self):
        log = logging.getLogger()
        log.setLevel('INFO')
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        log.addHandler(handler)
        self.log = log

    def CreateTable(self):
        self.session.execute("CREATE KEYSPACE IF NOT EXISTS "+ self.ReferenceName +" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };")
        self.session.execute("DROP TABLE IF EXISTS "+ self.ReferenceName +"."+ DReferenceContentTableName +";")
        self.session.execute("CREATE TABLE "+ self.ReferenceName + "."+ DReferenceContentTableName +" (blockid bigint, offset bigint, size int, value text, PRIMARY KEY(blockID));")
        if (DDebug):
            self.log.info(self.ReferenceName + "."+ DReferenceContentTableName +" Table Created !!!")

    def ReadFileChunk(self):
        """Lazy function (generator) to read a file piece by piece.
        Default chunk size: 1k."""
        global read_chrs, read_data
        while True:
            if (DDebug):
                print("ReadFileChunk2 {} {}".format(read_chrs, read_data))
            while (read_chrs < self.BlockSize):
                line = self.file.readline().rstrip('\n').upper()
                if not line:
                    break
                read_data = read_data + line
                read_chrs = read_chrs + len(line)
                if (DDebug):
                    print("Read Line: {} {}".format(read_chrs, read_data))
            if (read_chrs==0):
                if (DDebug):
                    print("No more Data: {} {}".format(read_chrs, read_data))
                break
            elif (read_chrs<self.BlockSize):
                read_chrs = 0
                data = read_data
                read_data =''
                if (DDebug):
                    print("No Full Block: {} {}".format(read_chrs, data))
                yield data
            else: 
                data = read_data[0:self.BlockSize]
                read_chrs = read_chrs - self.BlockSize
                read_data = read_data[self.BlockSize:]
                if (DDebug):
                    print("Full Block : {} {}".format(read_chrs, read_data))
                    print("Returning data: {}".format(data))
                yield data
                

    def WriteReferenceFile(self):
        
        GZIP_MAGIC_NUMBER = "1f8b"
        self.file = open(self.ReferenceFilename)
        is_gzip = self.file.read(2).encode("hex") == GZIP_MAGIC_NUMBER
        
        if (is_gzip):
            self.file = gzip.open(self.ReferenceFilename, 'rb')
        else:
            self.file = open(self.ReferenceFilename, 'rt')

        # Check for header line
        header = self.file.readline()        
        if (header and header[0]!='>'):
            self.file.seek(0)

        offset = 0
        self.InitStatement()
        for chunk in self.ReadFileChunk():
            chunk_size = len(chunk)
            self.InsertReferenceRow(chunk, offset, chunk_size)
            offset = offset + chunk_size
            #print(offset),
            
        self.file.close()

        
    def InitStatement(self):
        self.prepared_sql = self.session.prepare("INSERT INTO "+ self.ReferenceName + "."+ DReferenceContentTableName +" (blockid, offset, size, value)  VALUES (?,?,?,?)")
        self.future = None

        
    def InsertReferenceRow(self, data, offset, data_size):
        #self.session.execute(self.prepared_sql.bind((offset/self.BlockSize, offset,data_size, data)))
        if DFutureCheck:
            if self.future:
                try:
                    results = self.future.result()
                except Exception:
                    print("InsertReferenceRow::Error Checing asincronous insert")
        
        self.future = self.session.execute_async(self.prepared_sql.bind((offset/self.BlockSize, offset,data_size, data)))
        #print("Block {} -> {}, {}, {}".format(offset/self.BlockSize, offset,data_size, data))


    def CreateReferencoContentTable(self):
        self.CreateCassandraSession()
        self.SetLogger()
        self.CreateTable()
        self.WriteReferenceFile()
        print("Done")
    
        
## Testing 
if (DDoTesting):    
       
    # Test 1a: Calculate Query's keys & desplazaments (with header line)
    print("Test 1a: Create reference content")
    referenceFilename = '../Datasets/References/Example.txt'
    referenceName = str.lower("Example")
    contentBlockSize = 500
    obj = SparkBlast_CreateReferenceContent(referenceFilename, referenceName, contentBlockSize)
    obj.CreateReferencoContentTable()
    
    error

## Main function
if __name__ == '__main__':

    ## Process parameters. (https://docs.python.org/2/library/argparse.html)
    ## Blast_CreateReferenceContent <Reference_Files> [ReferenceName] [ContentBlockSize=1000]
    if (len(sys.argv)<2):
        print("Error parametes. Usage: Blast_CreateReferenceContent <Reference_Files> [ReferenceName=ReferenceFileName] [ContentBlockSize=1000].\n")
        sys.exit(1)

    referenceFilename = sys.argv[1]
    base = os.path.basename(referenceFilename).lower()
    referenceName, ext = os.path.splitext(base)    
    contentBlockSize = DContentBlockSize
    if (len(sys.argv)>2):
        referenceName = sys.argv[2]
    if (len(sys.argv)>3):
        contentBlockSize = int(sys.argv[3])       

    # Execute Main functionality
    print("{}({}, {}, {}).".format(sys.argv[0], referenceFilename, referenceName, contentBlockSize))
    
    obj = SparkBlast_CreateReferenceContent(referenceFilename, referenceName, contentBlockSize)
    obj.CreateReferencoContentTable()
