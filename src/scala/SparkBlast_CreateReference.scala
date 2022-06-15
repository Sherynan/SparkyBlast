package org.tfm.sparkScala.Blast_CreateReferenceContent

import com.datastax.driver.core.{Cluster, Session}
import org.apache.hadoop.conf
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}
import org.tfm.sparkScala.Blast_CreateReferenceContent.SparkBlast_DoQuery.DCassandraNodes

import java.io.{BufferedWriter, FileWriter}
import java.net.InetAddress
import java.nio.file.{Files, Paths}
import java.text.{DateFormat, SimpleDateFormat}
import java.util
import java.util.{Calendar, Date}
import scala.collection.JavaConverters.asJavaCollectionConverter
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.math.BigDecimal.RoundingMode

/**
 * Cassandra driver dependencies
 */


object SparkBlast_CreateReference {

  // Constants
  val DDoTesting = false
  val DDebug = false
  val DTiming = true
  val APP_NAME = "SparkBlast_CreateHash_"
  val DKeySize = 21
  //val DReferenceFilename = "../Datasets/References/GRCh38_latest_genomic_10K.fna"
  /** Environment de la feina, url del pc de la feina */
  //val DReferenceFilename = "../docs/Example2.txt"
  /** Environment de casa, url del pc de la feina */
  val DReferenceFilename = "C:/Users/oscar/Desktop/tfmTest/sparkScalaTest1/src/main/scala/org/tfm/sparkScala/docs/Example2.txt"
  //val DReferenceFilename = "C:/Users/oscar/Desktop/SparkyBlastTest/SparkyBlast/src/hdfs/Tmp/Datasets/References/Example2.txt"
  val DOutputFilename = "C:/Users/oscar/Desktop/SparkyBlastTest/SparkyBlast/src/hdfs/Tmp/Output/GRCh38_latest_genomic_10K"
  // val DOutputFilename = "hdfs://babel.udl.cat:8020/user/nando/output/references/GRCh38_latest_genomic/"
  val DReferenceName = "blast"
  val DReferenceHashTableName = "hash"
  val DReferenceContentTableName = "sequences"
  val DCreateBlocksDataFrame = true
  val DCreateWindowWithPartitions = false
  val DNumberPartitions = 1
  val DMaxNumberStages = 4
  val DMin_Lines_Partition = 200
  val DPartitionBlockSize: Int = 128 * 1024
  val DContentBlockSize = 1000
  val DHashGroupsSize = 10000000

  // Types
  // Enum Methods:
  val ECreate2LinesData = 1
  val ECreate1LineDataWithoutDependencies = 2
  val ECreateBlocksData = 3
  val DDefaultMethod: Int = ECreate2LinesData

  // Content Creation Methods
  val EContentCreationSpark = 1
  val EContentCreationPython = 2
  val DDefaultContentCreationMethod: Int = EContentCreationPython
  val DCalculateStageStatistics = true
  val DCalculateStatistics = true

  // Global Variables
  var Method: Int = DDefaultMethod
  var CreateWindowWithPartitions: Boolean = DCreateWindowWithPartitions
  var BlockSize: Int = DPartitionBlockSize
  var ContentBlockSize: Int = DContentBlockSize
  var ContentCreationSpark: Int = DDefaultContentCreationMethod
  var HashGroupsSize: Int = DHashGroupsSize
  var StatisticsFileName: String = ""
  var ReferenceName: String = DReferenceName
  var ReferenceFilename: String = DReferenceFilename
  var KeySize: Int = DKeySize
  var Local: Boolean = false
  var t0: Long = 0
  var t1: Long = 0
  var key_size_bc: Broadcast[Long] = null
  var hash_groups_size_bc: Broadcast[Long] = null
  var gt0_bc: Broadcast[Long] = null
  var NumberPartitions: Int = DNumberPartitions

  case class data_GenClass(size: Int, lines: String, offset: BigInt)
  /**
   * ## Functions ##
   * ###############
   */


  def createReference(spark: SparkSession, keySize: Int, referenceFilename: String, referenceName: String, hashGroupsSize: Int): Unit = {
    /** Getting spark context from session */
    val sc = spark.sparkContext
    /** Broadcast Variables */
    t0 = System.currentTimeMillis() / 1000 /* Converting Millis to seconds, we want time in seconds */

    // hmmmm /* Using .value because pyspark broadcast returns broadcast value instead of scala's broadcast Object, we use .value to get that value */
    gt0_bc = sc.broadcast(t0)
    key_size_bc = sc.broadcast(keySize)
    hash_groups_size_bc = sc.broadcast(hashGroupsSize)

    if (DTiming) {
      val date_time: Date = Calendar.getInstance().getTime
      val dateFormat: DateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
      //var conf = sc.getConf.getAll
      val executors = sc.getConf.get("spark.executor.instances")
      val cores = sc.getConf.get("spark.executor.cores")
      val memory = sc.getConf.get("spark.executor.memory")
      println("++++++++++++ INITIAL STATISTICS +++++++++++++ " + dateFormat.format(date_time) +
        "\n+ Reference: " + referenceName + "  \tFile:" + referenceFilename + " ." +
        "\n+ Key Size: " + keySize + "   \tMethod: " + Method + " ." +
        "\n+ Num Executors: " + executors + "  \tExecutors/cores: " + cores + "\tExecutor Mem: " + memory + "." +
        "\n+ Hash Groups Size: " + HashGroupsSize + "   \tPartition Size: " + BlockSize + "  \tContent Block Size: " + ContentBlockSize + "." +
        "\n++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    }


    /**
     * Create Cassandra reference table ##TODO preguntar fcores, variable duplicada y newApihadoopFile
     * Void method no need to encapsulate it in a variable
     */
    CreateCassandraReferenceTables(referenceName)

    /**
     * Read Reference file (offset,line) from hdfs
     *  Convert rdd to string rdd, to use it without exceptions
     */
    var reference_rdd: RDD[(Long, String)] = sc.newAPIHadoopFile(referenceFilename,
      classOf[TextInputFormat], classOf[LongWritable], classOf[Text], new conf.Configuration())
      .map( row => {
        (row._1.get(), row._2.toString)
      })

    /**
     * Show reference rdd
     */
    println(reference_rdd.collect().mkString("Array(", ", ", ")"))

    if (DCalculateStageStatistics) {
      println("Calculating statistics activated")
      reference_rdd.persist()
      println("### RDD PERSISTED ###")
    }
    println("@@@@@ Reference size: " + reference_rdd.count())

    if (DDebug) {
      println("Reference:: Input file has " + reference_rdd.count() + " lines and " + reference_rdd.getNumPartitions + " partitions: ")
      println("Reference:: First 10 records: " + reference_rdd.take(10).mkString("Array(", ", ", ")"))
      //reference_rdd.count()
    }

    /**
     *  Create de Reference Content Table
     *   Problem: The reference content table has to be created using a standalone python app using linux file.
     *   dfc = CreateReferenceContent(reference_rdd, sqlContext, ReferenceFilename, ContentBlockSize, ReferenceName)
     */
    var t1a = System.currentTimeMillis() / 1000

    /**
     * Create de Reference Index dataframe
     *
     */

    var df : DataFrame = CreateDataFrame(reference_rdd, spark)
    if(DCalculateStageStatistics) {
      df.persist()
      println("@@@@ Dataframe size: " + df.count())
    }

    /** Calculate keys */
    t1 = System.currentTimeMillis() / 1000
    df.show(3)
    val dataList: ListBuffer[List[((String, BigInt), BigInt)]] = iterateDF(df, spark)
    //GEN REF. KEYS LIKE AN RDD Documentar cambios RDD df.rdd.flatmap to list.flatten
    //https://users.scala-lang.org/t/convert-a-list-list-string-into-rdd-string/7828
    println(iterateDF(df, spark).toList)
    val result: RDD[((String, BigInt), BigInt)] = spark.sparkContext.parallelize(dataList.flatten)
    println(result.take(100).mkString("Array(", ", ", ")"))

    ShowBalanceStatistics(result)

    val rowsResultRDD : RDD[Row] = result.groupByKey().map(r => {
      val block : Int = r._1._2.toInt
      var intValuesList = new ListBuffer[Int]()
      for(elem <- r._2){
        intValuesList+=elem.toInt
      }
      Row(r._1._1, block, intValuesList.toList)
    })
    if (DCalculateStageStatistics) {
      rowsResultRDD.persist()
      println("@@@@@ Keys generated: " + rowsResultRDD.count())
    }

    val t2 = System.currentTimeMillis() / 1000
    val simpleSchema = new StructType()
      .add("seq", StringType)
      .add("block", IntegerType)
      .add("value", ArrayType(IntegerType))

    if(false) {
      rowsResultRDD.persist()
      println(rowsResultRDD.take(10).mkString("Array(", ", ", ")"))
      println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      println("Spark Version: " + spark.sparkContext.version + " .")
      println("Reference:: Result has " + rowsResultRDD.count() + " keys and " + " partitions: " + rowsResultRDD.getNumPartitions)
      val newPartitions = (result.count()/100.0).toInt

      var resultDF : DataFrame = null
      if( newPartitions > 200 ) {
        println(" New Partitions: " + newPartitions + " .")
        resultDF = spark.createDataFrame(rowsResultRDD, simpleSchema).repartition(newPartitions)
      } else {
        resultDF = spark.createDataFrame(rowsResultRDD, simpleSchema)
      }
      println("Reference:: Result has " + resultDF.count() + " keys and " + " partitions: " + resultDF.rdd.getNumPartitions)
      println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    }

    /**
     * Calculate Reference keys and write to cassandra
     * result.toDF().write.format("org.apache.spark.sql.cassandra").mode('append').options(table=DReferenceHashTableName, keyspace=ReferenceName).save()
     **/
    val resultDF = spark.createDataFrame(rowsResultRDD, simpleSchema)
    resultDF.write.format("org.apache.spark.sql.cassandra")
      .mode("overwrite")
      .options(Map("table" -> DReferenceHashTableName, "keyspace" -> ReferenceName))
      .option("confirm.truncate", "true")
      .save()

    val tc = System.currentTimeMillis() / 1000

    if (DTiming) {
      val date_time: Date = Calendar.getInstance().getTime
      val dateFormat: DateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")

      val executors = sc.getConf.get("spark.executor.instances")
      val cores = sc.getConf.get("spark.executor.cores")
      val memory = sc.getConf.get("spark.executor.memory")

      val totalTime: BigDecimal = BigDecimal(tc - t0).setScale(3, RoundingMode.HALF_UP)
      val dataReadTime: BigDecimal = BigDecimal(t1a - t0).setScale(3, RoundingMode.HALF_UP)
      val dataFrameTime: BigDecimal = BigDecimal(t1 - t1a).setScale(3, RoundingMode.HALF_UP)

      val keyCalcTime: BigDecimal = BigDecimal(t2 - t1).setScale(3, RoundingMode.HALF_UP)
      val cassandraWriteTime: BigDecimal = BigDecimal(tc - t2).setScale(3, RoundingMode.HALF_UP)

      println("########################## FINAL STATISTICS ########################## " + dateFormat.format(date_time) +
        "\n# Reference: " + referenceName + "  \tFile:" + referenceFilename + " ." +
        "\n# Key Size: " + keySize + "   \tMethod: " + Method + " ." +
        "\n# Num Executors: " + executors + "  \tExecutors/cores: " + cores + "\tExecutor Mem: " + memory + "." +
        "\n# Hash Groups Size: " + HashGroupsSize + "   \tPartition Size: " + BlockSize + "  \tContent Block Size: " + ContentBlockSize + "." +
        "\n# Total Time: " + totalTime + "\tData Read Time: " + dataReadTime + "\tData Frame Time: "+ dataFrameTime +" ." +
        "\n# # Key Calc Time: " + keyCalcTime + "\tCassandra Write Time: " + cassandraWriteTime + " ." +
        "\n############################################################################################")

      if(!StatisticsFileName.equals("")) {
        writeStatistics(StatisticsFileName, ReferenceFilename, ReferenceName, KeySize, date_time, result.count().toInt,
          result.getNumPartitions , (tc-t0).toInt, (t1a-t0).toInt, (t1-t1a).toInt, (t2-t1).toInt, (tc-t2).toInt, spark)
      }
    }
    println("Done.")
  }

  def iterateDF(df : DataFrame, spark: SparkSession) : ListBuffer[List[((String, BigInt), BigInt)]] = {
    import spark.implicits._
    val dataList = new ListBuffer[List[((String, BigInt), BigInt)]]
    df.as[data_GenClass].take(df.count.toInt).foreach(t => {
      dataList.append(generateReferenceKeys(mutable.Map(("size", t.size), ("lines", t.lines), ("offset", t.offset))).toList)
    })
    dataList
  }

  /**
   * Functions to get spark conf data
   */

  /** Method that just returns the current active/registered executors
   * excluding the driver.
   *
   * @param sc The spark context to retrieve registered executors.
   * @return a list of executors each in the form of host:port.
   */

  def currentActiveExecutors(sc: SparkContext): Seq[String] = {
    val allExecutors = sc.getExecutorMemoryStatus.keys
    val driverHost: String = sc.getConf.get("spark.driver.host")
    allExecutors.filter(!_.split(":")(0).equals(driverHost)).toList
  }

  def ShowBalanceStatistics(rdd: RDD[((String, BigInt), BigInt)]): Unit = {
    if(true || !DDebug){
      return
    }
    println("@@@@@ Offsets number distribution among partitions: ")
    var partition : Int = 1
    var total : Int = 0
    var max : Int = 0

    for(par <- rdd.glom().collect()) {
      val alig = par.length
      println("@@@@@ Partition " + partition + " queries: " + par.length + " aligmnets: " + alig + " .")
      partition += 1
      total += alig
      if( alig > max) {
        max = alig
      }
      println("@@@@@ TOTAL offsets: " + total + " .")
      val numPart = rdd.getNumPartitions
      println("@@@@@ Number of partitions: " + numPart + " .")

      if (numPart >= 1 && total > 0) {
        val ratio = total.toFloat/numPart.toFloat
        val roundedRatio : BigDecimal = BigDecimal(((max-ratio)/ratio)*100.0).setScale(3, RoundingMode.HALF_UP)
        println("@@@@@ Number of partition: " + numPart + " Mean aligns/part: " + total/numPart + " Unbalancing: " + roundedRatio + "%.")
      } else {
        println("@@@@@ Number of partition: " + numPart + " Mean aligns/part: " + 0 + " Unbalancing: " + 0 + "%.")
      }
    }

  }

  /**
   * Create the sequence and hash tables for the Reference
   */
  def CreateCassandraReferenceTables(referenceName: String): Unit = {
    /**cluster = Cluster(['babel.udl.cat', 'client1.babel', 'client2.babel', 'client3.babel', 'client4.babel',' client5.babel'])
    #cluster = Cluster(['192.168.1.1', '192.168.1.2', '192.168.1.3', '192.168.1.4', '192.168.1.5', '192.168.1.6'])
    #cluster = Cluster(['192.168.1.1'])*/

    val nodes : util.Collection[InetAddress] = DCassandraNodes.map(ip => InetAddress.getByName(ip)).asJavaCollection
    val cluster: Cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    implicit val session: Session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS " + referenceName + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };")
    session.execute("DROP TABLE IF EXISTS " + referenceName + "." + DReferenceHashTableName + ";")
    session.execute("CREATE TABLE " + referenceName + "." + DReferenceHashTableName + " (seq text, block int, value list<bigint>, PRIMARY KEY(seq,block));")

    /**
     * session.execute("DROP TABLE IF EXISTS "+ ReferenceName +"."+ DReferenceContentTableName +";")
     * session.execute("CREATE TABLE "+ ReferenceName + "."+ DReferenceContentTableName +" (blockid bigint, offset bigint, size int, value text, PRIMARY KEY(blockID));")
     */
    session.close()
    cluster.close()

    if (DDebug) {
      println("Cassandra Database Created.")
    }
  }

  /**
   * Create dataframe and delete first line header if exists
   */
  def CreateDataFrame(reference_rdd: RDD[(Long,String)], spark: SparkSession): DataFrame = {
    import org.apache.spark.sql._
    /** Create Dataframe */
    import spark.implicits._
    implicit val enc: Encoder[data_GenClass] = Encoders.product[data_GenClass]
    val reference_df : DataFrame = reference_rdd.toDF("file_offset", "line")
    //val refDf5000: DataFrame = spark.sparkContext.parallelize(reference_rdd.take(5000)).toDF("file_offset", "line")
    /** Delete first line header if exist */
    var header = reference_df.first()
    var headerSize = 0
    var lineVal : String = header.getAs("line")
    var df1 : DataFrame = null
    var df : DataFrame = null

    if(lineVal.startsWith(">")) {
      headerSize = lineVal.length + 1
      df1 = reference_df.filter(reference_df("file_offset") =!= 0)
    }
    else {
      df1 = reference_df
    }

    if( Method == ECreate2LinesData ) {
      df = Create2LinesDataFrame(df1, headerSize, BlockSize, spark)
    }
    else if( Method == ECreate1LineDataWithoutDependencies ) {
    df = Create1LineDataFrameWithoutDependencies(df1, headerSize)
    }
    else if( Method == ECreateBlocksData ) {
      df = CreateBlocksDataFrame(df1, headerSize, BlockSize, spark)
    }
    df
  }

  def Create2LinesDataFrame(df: DataFrame, headerSize: Int, blockSize: Int, spark: SparkSession): DataFrame = {
    t1 = System.currentTimeMillis() / 1000
    var myWindow : WindowSpec= null
    var df2lines : DataFrame = null
    var df2lines2 : DataFrame = null
    var df2lines3 : DataFrame = null

    if(false && CreateWindowWithPartitions && blockSize > 0) {
      df2lines = df.withColumn("block", (df("file_offset") / blockSize).cast(IntegerType))
      myWindow = Window.partitionBy("block").orderBy("file_offset")
      println("Spark shuffle partitions:" + spark.sqlContext.getConf("spark.sql.shuffle.partitions"))
    } else {
      myWindow = Window.partitionBy().orderBy("file_offset")
    }

    df2lines = df.withColumn("line", upper(df("line")))
      .withColumn("id", row_number.over(myWindow))
    df2lines2 = df2lines.withColumn("next_line", lag(df2lines("line"), -1).over(myWindow))
    df2lines3 = df2lines2.withColumn("size", length(df2lines2("line")))
      .withColumn("lines", when(isnull(df2lines2("next_line")), df2lines2("line"))
        .otherwise(concat(df2lines2("line"), df2lines2("next_line")))
      )
      .withColumn("offset", (df2lines2("file_offset") - headerSize) - (df2lines2("id") - 1))
      .drop(df2lines2("line"))
      .drop(df2lines2("next_line"))
      .drop(df2lines2("file_offset"))
      .drop(df2lines2("id"))

    /** Calculate Number of Partitions */
    var executors = spark.sparkContext.getConf.get("spark.executor.instances")
    var cores = spark.sparkContext.getConf.get("spark.executor.cores")
    var totalCores = 0
    if(executors == null || cores == null) {
      totalCores = 1
    } else {
      totalCores = executors.toInt * cores.toInt
    }

    println("@@@@ Total cores: " + totalCores)
    var maxPartitions = DMaxNumberStages * totalCores
    println("@@@@ Max partitions: " + maxPartitions)
    var lines = df2lines3.count()
    var linesPartition = lines/maxPartitions
    println("@@@@ Lines: " + lines + " Lines/Part: " + linesPartition)

    if(linesPartition > DMin_Lines_Partition) {
      NumberPartitions = maxPartitions
      println("@@@@ MAX PARTITIONS -- Number of partitions: " + NumberPartitions)
    } else {
      NumberPartitions = (lines/DMin_Lines_Partition).toInt
    }

    if(NumberPartitions == 0) {
      NumberPartitions = 1
    }
    println("@@@@ Number of partitions: " + NumberPartitions)

    df2lines3 = df2lines3.repartition(NumberPartitions)

    if(DTiming){
      //println("Time required for read and prepare dataframe with " + df2lines3.count() + "rows using " + df2lines3.rdd.getNumPartitions + "partitions in " + round(System.currentTimeMillis()/1000) - gt0_bc.value)
        //round((System.currentTimeMillis()/1000) - gt0_bc.value, 3))
      /** HALF_UP rounding mode is the one commonly taught at school:
       * 1. if the discarded fraction is >= 0.5, round up;
       * 2. if the discarded fraction is < 0.5, then round down.
       */
      val time: BigDecimal = BigDecimal((System.currentTimeMillis() / 1000) - t1).setScale(3, RoundingMode.HALF_UP)
      println("Time required for read and prepare dataframe in " + time + " seconds.")
      //print("Time required for read and prepare dataframe in {} seconds.\n".format(round(time() - t1,3)))
    }
    df2lines3
  }

  def Create1LineDataFrameWithoutDependencies(df: DataFrame, headerSize: Int): DataFrame = {
    /**  Create Blocks of lines to avoid dependencies with the previous line.
         Pyspark Ref: https://stackoverflow.com/questions/49468362/combine-text-from-multiple-rows-in-pyspark
         Converted to scala spark format.
     */
    var myWindow : WindowSpec= null
    var df1lines : DataFrame = null
    var df1lines2 : DataFrame = null

    myWindow = Window.partitionBy().orderBy("file_offset")
    df1lines = df.withColumn("lines", upper(df("line")))
      .withColumn("id", row_number().over(myWindow))
    df1lines2 = df1lines.withColumn("size", length(df1lines("lines") - DKeySize))
      .withColumn("offset", (df1lines("file_offset") - headerSize) - (df1lines("id")-1))
      .drop(df1lines("line"))
      .drop(df1lines("file_offset"))
      .drop(df1lines("id"))

    /**
     * df1lines2.persist()
     * df1lines2.rdd.getNumPartitions()
     * println("Number of total rows: " + df1lines2.count() + "with " + df1lines2.rdd.glom().count() + "partitions")
     * println("df1lines2: ")
     * df1lines2.show(20)
     * val rdd = sc.cassandraTable("test", "words")
     * val time: BigDecimal = BigDecimal((System.currentTimeMillis() / 1000) - gt0_bc.value)
     *    .setScale(3, RoundingMode.HALF_UP)
     * println("Time required for read and prepare dataframe with " + df1lines2.count() +
     *    "rows using " + df1lines2.rdd.getNumPartitions + "partitions in " + time + " seconds.")
     */

    if(DTiming) {
      val time: BigDecimal = BigDecimal((System.currentTimeMillis() / 1000) - gt0_bc.value).setScale(3, RoundingMode.HALF_UP)
      println("Time required for read and prepare dataframe with " + df1lines2.count() +
        " rows using " + df1lines2.rdd.getNumPartitions + " partitions in " + time + " seconds.")
    }

    df1lines2

  }

  def CreateBlocksDataFrame(df: DataFrame, headerSize: Int, blockSize: Int, spark: SparkSession): DataFrame = {
    t1 = System.currentTimeMillis() / 1000
    var myWindow : WindowSpec= null
    var dfblock : DataFrame = null
    var dfblock0 : DataFrame = null
    var dfblock1 : DataFrame = null
    var dfblock2 : DataFrame = null
    var dfblock3 : DataFrame = null


    if(DDebug) {
      println("Method: ECreateBlocksData (Value = " + ECreateBlocksData + " )")
      df.show(10)
    }
    //return CreateBlocksDataFrame2(dfc)

    val keySize : Long = key_size_bc.value

    if(false && CreateWindowWithPartitions && blockSize > 0) {
      dfblock = df.withColumn("block", df("file_offset") / blockSize)
      myWindow = Window.partitionBy("block").orderBy("file_offset")
      println("Spark shuffle partitions:" + spark.sqlContext.getConf("spark.sql.shuffle.partitions"))
    } else {
      myWindow = Window.partitionBy().orderBy("file_offset")
    }

    /**
     * dfblock.show(20)
     * my_window = Window.partitionBy().orderBy("offset")
     * dfblock1 = dfblock.select(dfblock("value").substr(0, keySize.toInt).alias("prefix"))
     */
    dfblock0 = dfblock.withColumn("lines", upper(dfblock("line")))
    dfblock1 = dfblock0.withColumn("prefix",dfblock0("lines").substr(0, keySize.toInt - 1))
    dfblock2 = dfblock1.withColumn("next_line", lag(dfblock1("prefix"),-1).over(myWindow))
    dfblock3 = dfblock2.withColumn("lines", when(isnull(dfblock2("next_line")), dfblock2("lines"))
        .otherwise(concat(dfblock2("lines"), dfblock2("next_line"))))
      .withColumn("offset", dfblock2("file_offset") - headerSize)
    dfblock3 = dfblock3.withColumn("size", length(dfblock3("lines")))
      .drop("prefix")
      .drop("next_line")
      .drop("line")
      .drop(dfblock3("file_offset"))
    /** Testing purposes ::: dfblock3.sort(col("offset").asc()).show(10) */
    dfblock3.sort(col("offset").asc()).show(10)

    if(DTiming) {
      val time: BigDecimal = BigDecimal((System.currentTimeMillis() / 1000) - t1).setScale(3, RoundingMode.HALF_UP)
      println("Time required for read and prepare dataframe in " + time + " seconds.")
    }

    dfblock3
  }

  /**
   * Crates blocks of lines dataframe from imput file rdd (deprecated).
   */
  def CreateBlocksDataFrame2(df : DataFrame): DataFrame = {
    var dfblock : DataFrame = null

    if (DDebug) {
      println("Method: ECreateBlocksData2 (Value: " + ECreateBlocksData + " )" )
    }
    t1 = System.currentTimeMillis() / 1000
    dfblock = df.withColumn("lines", dfblock("value"))
      .drop("value")

    if (DDebug) {
      println("Dataframe done with " + dfblock.count() + " rows using " + dfblock.rdd.getNumPartitions + " partitions in seconds")
    }

    if (DTiming) {
      val time: BigDecimal = BigDecimal((System.currentTimeMillis() / 1000) - t1).setScale(3, RoundingMode.HALF_UP)
      println("Time required for read and prepare dataframe in " + time + " seconds.")
    }

    dfblock
  }

  def AddCassandraKey(key : Integer, offset : Integer): Unit = {
    var listText: String = offset.toString
    println("Key=" + key + ", listText=" + listText)

    val cluster = Cluster.builder()
      .addContactPoints("192.168.1.1","192.168.1.2", "192.168.1.3", "192.168.1.4", "192.168.1.5", "192.168.1.6")
      .build()
    implicit val session: Session = cluster.connect()
    session.execute("update blast.sequences set value = value + [" + listText + "] where seq ="+ key + ";")

    /**
     * rddUpdateSeq = sc.parallelize([Row(seq=key,value=listText)])
     * rddUpdateSeq.toDF().write\
     * .format("org.apache.spark.sql.cassandra")\
     * .mode('append')\
     * .options(table="sequences", keyspace="blast")\
     * .save()
     */
    session.close()
    cluster.close()
    //if nothing is set it returns an empty val like if it was an empty return.
  }

  def generateReferenceKeys(record: mutable.Map[String, Any]): ListBuffer[((String, BigInt), BigInt)] = {
    generateReferenceKeysR(record("offset").asInstanceOf[BigInt], record("size").asInstanceOf[Int], record("lines").asInstanceOf[String], record("lines").asInstanceOf[String].length)
  }

  def generateReferenceKeysR(offset: BigInt, size: Int, lines: String, linesSize : Int): ListBuffer[((String, BigInt), BigInt)] = {
    val keySize : Long  = key_size_bc.value
    val hashGroupsSize : Long = hash_groups_size_bc.value
    var keysList = new ListBuffer[((String, BigInt), BigInt)]
    //tg1 = System.currentTimeMillis() / 1000
    /**
     * Calculate the first and last keys displacements.
     */
    var firstKey : Int = 0
    var lastKey : Int = 0
    var forbiddenKey : String = "N" * keySize.toInt

    if(size != linesSize) {
      /** Internal lines. */
      lastKey = size
    } else {
      lastKey = size - keySize.toInt + 1
    }
    print("lines_size :::", linesSize)

    for(k <- firstKey until lastKey) {
      /**
       * Add key to casandra
       * AddCassandraKey(lines[k:k+Key_size], offset+k)
       *
       * Add key to scala list
       */
      /**
        #Keys.append({'Key':lines[k:k+Key_size],'Offset':offset+k})
        #Keys.append((lines[k:k+Key_size],str(offset+k)))
        #Keys.append({'seq':lines[k:k+KeySize],'value':[offset+k]})
        #Keys.append(Row(seq=lines[k:k+KeySize],value=[offset+k]))
      */
      if(lines.substring(k, k+KeySize) != forbiddenKey) {
        if((offset+k) < 0) {
          println("ERROR::generateReferenceKeysR (offset="+offset+", size="+size+", lines="+lines.mkString("Array(", ", ", ")")+
            ", linesSize="+linesSize+", offset+k="+offset+k)
        }
        //error
        if(false && lines.substring(k, k+KeySize) == "GAATGCTGTGT") {
          println("DEBUG::generateReferenceKeysR GAATGCTGTGT (offset="+offset+", size="+size+", lines="+lines.mkString("Array(", ", ", ")")+
            ", linesSize="+linesSize+", offset+k="+offset+k)
        }

        keysList.append(((lines.substring(k, k+KeySize),(offset+k)/hashGroupsSize), offset+k))
      }

      /*
      val time: BigDecimal = BigDecimal((System.currentTimeMillis() / 1000) - gt0_bc.value).setScale(3, RoundingMode.HALF_UP)
      println("\rProcessing " + keysList.length + " keys from offset " + offset + " in " + time + " secs.\t"
      */

    }
    println("@@@ KEYS LIST ::" + keysList.mkString("Array(", ", ", ")"))
    keysList
  }

  def writeStatistics(statisticsFileName : String, referenceFilename : String, referenceName : String, keySize : Integer,
                      date : Date, keysNumber : Integer, partitions : Integer, totalTime : Integer, readTime : Integer,
                      dfTime : Integer, keyTime : Integer, casTime: Integer, spark: SparkSession): Unit = {
    val DStatisticsFileHeader = "#Procedure; Reference File Name ; Reference Name ; Date  ; Number of executors ; Cores/Executor ; Memory/Executor ; Total Time (sec) ; Data Read Time (sec) ; Data Frame Time (sec) ; Key Calc Time (sec) ; Cassandra Write Time (sec) ; Key Size; Number of Keys ; Number of partitions ; Method ; Hash Group Size ; Hash Groups Size ; Block Size ; Content Block Size ; "
    var DHdfsHomePath = ""
    var DHdfsTmpPath = ""
    if(Local){
      DHdfsHomePath = "C:/Users/oscar/Desktop/SparkyBlastTest/SparkyBlast/src/hdfs/"
      val DHdfsTmpPath = DHdfsHomePath + "Tmp/"
    } else {
      val DHdfsHomePath = "hdfs://babel.udl.cat/user/nando/"
      val DHdfsTmpPath = DHdfsHomePath + "Tmp/"
    }

    val executors = spark.sparkContext.getConf.get("spark.executor.instances")
    val cores = spark.sparkContext.getConf.get("spark.executor.cores")
    val memory = spark.sparkContext.getConf.get("spark.executor.memory")

    val newStats = "\"SparkBlast Create Hash\" ; " + referenceFilename + " ; " + referenceName + " ; " + date + " ; " +
      "" + executors + " ; " + cores + " ; " + memory + " ; " + totalTime + " ; " + readTime + " ; " + dfTime + " ; " +
      "" + keyTime + " ; " + casTime + " ; " + keySize + " ; " + keysNumber + " ; " + partitions + " ; " + Method + " ; " +
      "" + HashGroupsSize + " ; " + BlockSize + ContentBlockSize + " ; "

    if(false) {
      /**
       * Generate statistics in linux file
       */

      // JAVA FileWriter - scala has no approach to do it
      val statisticsFile = new java.io.File(statisticsFileName)
      val statisticsBw = new BufferedWriter(new FileWriter(statisticsFile))
      /*if(Files.exists(Paths.get(statisticsFileName))) {
        var statisticsFile = new File(statisticsFileName)
      } else {
        var statisticsFile = new File(statisticsFileName)
        var statisticsBw = new BufferedWriter(new FileWriter(statisticsFile))
        statisticsBw.write(DStatisticsFileHeader)
        statisticsBw.close()
      }*/
      if(Files.exists(Paths.get(statisticsFileName))) {
        statisticsBw.write(DStatisticsFileHeader)
      }
      /**
       * new_stats = "SparkBlast Create Hash ; " + referenceFilename + " ; " + referenceName + " ; " + str(totalTime) + " ; " + str(readTime) + " ; "+ str(dfTime) + " ; " + str(keyTime) + " ; " + str(casTime) + " ; " + str(keySize) + " ; " + str(keysNumber) + " ; " + str(partitions) + " ; " + str(Method) + " ; " + str(HashGroupsSize) + " ; " + str(BlockSize) + " ; " + str(ContentBlockSize) + " ;")
       */

      statisticsBw.write(newStats + "\n")
      statisticsBw.close()

      /**
       * # Copy statistics file into hdfs
       * #cmd = ['hdfs', 'dfs', '-put', statisticsFileName, DHdfsResultsPath]
       * #run_cmd(cmd)
       */
      println(newStats)
      println("Print number of executors: " + executors + "\tExecutor cores: " + cores + "\tExecutors memory: " + memory)
    }

    println("File "+ DHdfsHomePath + statisticsFileName + " ")

  }

  def runCmd(argsList : Array[String]): Unit = {
    //https://alvinalexander.com/scala/how-to-run-external-commands-processes-different-directory-scala/
    import scala.sys.process._
    val pb = Process(argsList.mkString(" "))
    //print("Running system command: " + argsList.mkString(" "))
    //run the process in background (detached) and get a Process instance
    pb.run()
  }

  /**
   * def run_cmd(args_list):
    #print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    print(proc)
    proc.communicate()
    return proc.returncode
   */

  def checkFile(hdfsFilePath : String): Unit = {
    scala.reflect.io.File(hdfsFilePath).exists

    if (!Local) {
      runCmd(Array("hdfs", "dfs", "-rm", "-R", hdfsFilePath))
    }
  }

  def removeFile(hdfsFilePath : String): Unit ={
    if(scala.reflect.io.File(hdfsFilePath).exists) {
      scala.reflect.io.File(hdfsFilePath).delete()
    }
    if (!Local) {
      runCmd(Array("hdfs", "dfs", "-rm", "-R", hdfsFilePath))
    }
  }

  def getFileSize(startPath: String, initSize: Long): Long = {
    val directory = new java.io.File(startPath)
    var totalSize : Long = initSize
    directory.listFiles.filter(_.isFile).foreach(totalSize += _.length())
    val pathList : List[String] = directory.listFiles.filter(_.isDirectory).map(_.getPath).toList
    pathList.foreach(path => totalSize = getFileSize(path, totalSize))
    totalSize
  }

  def initMethod(spark : SparkSession, keySize: Int, referenceFilename: String, referenceName: String, hashGroupsSize: Int): Unit = {
    println("INIT METHOD")
    println(getFileSize("C:/Users/oscar/Desktop/tfmTest/sparkScalaTest1/src/main/scala/org/tfm/sparkScala", 0))
    createReference(spark, keySize, referenceFilename, referenceName, hashGroupsSize)
  }

  def doTesting(spark : SparkSession) {
    if(DDoTesting){
      var keySize = DKeySize

      /**
       * Test 1
       * println("Testing: " + filename + ", " + keySize + ", " + referenceName)
       * TestCreateReferenceIndex(sc, keySize)
       */

      /**
       * Test 2: Generate reference to file
       */
      var inputfile = DReferenceFilename
      var outputfile = DOutputFilename
      /**
       *  CreateReferenceIndexToFile(sc, sqlContext, keySize, inputfile, outputfile)
       */

      /**
       * Test 3: Generate reference in Cassandra
       */
      var referenceFilenameT3 = "../Datasets/References/Example.txt"
      var referenceNameT3 = "Example".toLowerCase
      Method = ECreateBlocksData
      ContentBlockSize = 1000
      /**
       * customMain(spark.sparkContext, spark.sqlContext, keySize, referenceFilenameT3, referenceNameT3)
       */

      /**
       * Test 4a: Generate reference in Cassandra
       */
      var referenceFilenameT4a = "../Datasets/References/GRCh38_latest_genomic_5P.fna"
      var referenceNameT4a = "GRCh38_5P_3".toLowerCase
      Method = ECreate2LinesData
      ContentBlockSize = 100
      /**
       * customMain(spark.sparkContext, spark.sqlContext, keySize, referenceFilenameT4a, referenceNameT4a)
       */

      /**
       * Test 4b: Generate reference in Cassandra
       */
      var referenceFilenameT4b = "../Datasets/References/GRCh38_latest_genomic_10K.fna"
      var referenceNameT4b = "GRCh38_latest_genomic_10K".toLowerCase
      Method = ECreateBlocksData
      ContentBlockSize = 100
      /**
       * customMain(spark.sparkContext, spark.sqlContext, keySize, referenceFilenameT4b, referenceNameT4b)
       */

      /**
       * Test 4c: Generate reference in Cassandra
       */
      var referenceFilenameT4c = "../Datasets/References/GRCh38_latest_genomic_10K.fna"
      var referenceNameT4c = "GRCh38_latest_genomic_10K".toLowerCase
      Method = ECreateBlocksData
      ContentBlockSize = 100
      /**
       * customMain(spark.sparkContext, spark.sqlContext, keySize, referenceFilenameT4c, referenceNameT4c)
       */

      /**
       * Test 4d: Generate reference in Cassandra
       */
      var referenceFilenameT4d = "../Datasets/References/GRCh38_latest_genomic_10K.fna"
      var referenceNameT4d = "GRCh38_latest_genomic_10K".toLowerCase
      Method = ECreateBlocksData
      ContentBlockSize = 10000
      /**
       * customMain(spark.sparkContext, spark.sqlContext, keySize, referenceFilenameT4d, referenceNameT4d)
       */

      /**
       * END TESTING
       */
    }
  }

  def main(args: Array[String]): Unit = {
    /**
     * SparkBlast_CreateReference <Reference_Files> [Key_size=11] [ReferenceName] [Method]
     * [PartitionSize] [ContentBlockSize]
     */
    if (args.length < 2) {
      println("Error parametes. Usage: SparkBlast_CreateReference <Reference_Files> [Key_size={}] [ReferenceName] [Method={}]  [BlockSize={}] [ContentBlockSize={}] [HashGroupsSize={}] [StatisticsFileName].\n".format(DKeySize, DDefaultMethod, DPartitionBlockSize, DContentBlockSize, DHashGroupsSize))
      sys.exit(1)
    }
    println(args(1))

    ReferenceFilename = args(1)
    KeySize = DKeySize
    Method = DDefaultMethod
    BlockSize = DPartitionBlockSize
    ContentBlockSize = DContentBlockSize
    HashGroupsSize = DHashGroupsSize

    if (args.length > 2) {
      KeySize = args(2).toInt
    }
    if (args.length > 3) {
      ReferenceName = args(3).toLowerCase()
    }
    if (args.length > 4) {
      Method = args(4).toInt
    }
    if (args.length > 5) {
      CreateWindowWithPartitions = false
      BlockSize = args(5).toInt
    }
    if (args.length > 6) {
      ContentBlockSize = args(6).toInt
    }
    if (args.length > 7) {
      HashGroupsSize = args(7).toInt
    }
    if (args.length > 8) {
      StatisticsFileName = args(8)
    }
    if (args.length > 9) {
      Local = args(9).toBoolean
    }

    /**
     * Configure Spark
     */
    val spark = SparkSession
      .builder()
      .appName(APP_NAME + ReferenceName)
      .master("local[3]")
      .config("spark.executor.instances", 1)
      .config("spark.executor.cores", 2)
      .config("spark.executor.memory", "2gb")
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    //random.seed()

    t0 = System.currentTimeMillis() / 1000 /* Converting Millis to seconds, we want time in seconds */
    gt0_bc = sc.broadcast(t0)

    /**
     * Main program info
     */
    println("Arg0: " + args(0) +
      "\nReferenceFilename: " + ReferenceFilename +
      "\nKeySize: " + KeySize +
      "\nReferenceName: " + ReferenceName +
      "\nMethod: " + Method +
      "\nBlockSize: " + BlockSize +
      "\nContentBlockSize: " + ContentBlockSize +
      "\nHashGroupsSize: " + HashGroupsSize)

    /**
     * Calling methods
     */
    println(" ### MAIN ###")
    initMethod(spark, keySize = DKeySize, referenceFilename = DReferenceFilename, referenceName = "Sequences", hashGroupsSize = DHashGroupsSize)


  }


}

