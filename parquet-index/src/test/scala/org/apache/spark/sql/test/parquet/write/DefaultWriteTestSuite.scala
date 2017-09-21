package org.apache.spark.sql.test.parquet.write

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.In
import org.apache.spark.sql.test.parquet._
import org.apache.spark.sql.test.parquet.ss.implicits._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.{Partition, TaskContext}
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn

class DefaultWriteTestSuite extends FunSuite {
  private lazy val log = LogFactory.getLog(classOf[DefaultWriteTestSuite])

  test("default_write") {
    val df = ss.sparkContext.parallelize(testData).coalesce(1).toDF("key", "value")
    df.write.partitionBy("key").parquet(path)
  }

  test("file_scan") {
    val userSpecifiedSchema: Option[StructType] = None
    val partitionColumns: Seq[String] = Seq.empty
    val options = Map.empty[String, String]
    val caseInsensitiveOptions = CaseInsensitiveMap(options)
    val format = new ParquetFileFormat
    val fileStatusCache = FileStatusCache.getOrCreate(ss)
    val catalogTable: Option[CatalogTable] = None

    def checkColumnNameDuplication(
                                    columnNames: Seq[String], colType: String, caseSensitiveAnalysis: Boolean): Unit = {
      val names = if (caseSensitiveAnalysis) {
        columnNames
      } else {
        columnNames.map(_.toLowerCase)
      }
      if (names.distinct.length != names.length) {
        val duplicateColumns = names.groupBy(identity).collect {
          case (x, ys) if ys.length > 1 => s"`$x`"
        }
        log.warn(s"Found duplicate column(s) $colType: ${duplicateColumns.mkString(", ")}. " +
          "You might need to assign different column names.")
      }
    }

    def getOrInferFileFormatSchema(
                                    format: FileFormat,
                                    fileStatusCache: FileStatusCache = NoopCache): (StructType, StructType) = {
      lazy val tempFileIndex = {
        val allPaths = caseInsensitiveOptions.get("path") ++ Seq(path)
        val hadoopConf = ss.sessionState.newHadoopConf()
        val globbedPaths = allPaths.toSeq.flatMap { path =>
          val hdfsPath = new Path(path)
          val fs = hdfsPath.getFileSystem(hadoopConf)
          val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
          SparkHadoopUtil.get.globPathIfNecessary(qualified)
        }.toArray
        new InMemoryFileIndex(ss, globbedPaths, options, None, fileStatusCache)
      }
      val partitionSchema = if (partitionColumns.isEmpty) {
        // Try to infer partitioning, because no DataSource in the read path provides the partitioning
        // columns properly unless it is a Hive DataSource
        val resolved = tempFileIndex.partitionSchema.map { partitionField =>
          val equality = ss.sessionState.conf.resolver
          // SPARK-18510: try to get schema from userSpecifiedSchema, otherwise fallback to inferred
          userSpecifiedSchema.flatMap(_.find(f => equality(f.name, partitionField.name))).getOrElse(
            partitionField)
        }
        StructType(resolved)
      } else {
        if (userSpecifiedSchema.isEmpty) {
          val inferredPartitions = tempFileIndex.partitionSchema
          inferredPartitions
        } else {
          val partitionFields: Seq[StructField] = partitionColumns.map { partitionColumn =>
            val equality = ss.sessionState.conf.resolver
            userSpecifiedSchema.flatMap(_.find(c => equality(c.name, partitionColumn))).orElse {
              val inferredPartitions = tempFileIndex.partitionSchema
              val inferredOpt = inferredPartitions.find(p => equality(p.name, partitionColumn))
              inferredOpt
            }.getOrElse {
              throw new AnalysisException(s"Failed to resolve the schema for $format for " +
                s"the partition column: $partitionColumn. It must be specified manually."
              )
            }
          }
          StructType(
            partitionFields)
        }
      }

      val dataSchema =
        userSpecifiedSchema.map { schema =>
          val equality = ss.sessionState.conf.resolver
          StructType(
            schema.filterNot(f =>
              partitionSchema.exists(p => equality(p.name, f.
                name))))
        }.orElse {
          format.
            inferSchema(
              ss,
              caseInsensitiveOptions,
              tempFileIndex.allFiles())
        }
          .getOrElse {
            throw new
                AnalysisException(
                  s"Unable to infer schema for $format. It must be specified manually.")
          }

      checkColumnNameDuplication(
        (dataSchema ++ partitionSchema).map(_.name), "in the data schema and the partition schema",
        ss.sessionState.conf.caseSensitiveAnalysis)

      (dataSchema, partitionSchema)
    }

    val hadoopConf = ss.sparkContext.hadoopConfiguration
    val allPaths = caseInsensitiveOptions.get("path") ++ Seq(path)
    val globbedPaths = allPaths.flatMap { path =>
      val hdfsPath = new Path(path)
      val fs = hdfsPath.getFileSystem(hadoopConf)
      val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
      val globPath = SparkHadoopUtil.get.globPathIfNecessary(qualified)

      if (globPath.isEmpty) {
        throw new AnalysisException(s"Path does not exist: $qualified")
      }
      // Sufficient to check head of the globPath seq for non-glob scenario
      // Don't need to check once again if files exist in streaming mode
      if (!fs.exists(globPath.head)) {
        throw new AnalysisException(s"Path does not exist: ${globPath.head}")
      }
      globPath
    }.toArray
    val (dataSchema, partitionSchema) = getOrInferFileFormatSchema(format, fileStatusCache)
    val bucketSpec: Option[BucketSpec] = None
    val fileCatalog = if (ss.sessionState.conf.manageFilesourcePartitions &&
      catalogTable.isDefined && catalogTable.get.tracksPartitionsInCatalog) {
      val defaultTableSize = ss.sessionState.conf.defaultSizeInBytes
      new CatalogFileIndex(
        ss,
        catalogTable.get,
        catalogTable.get.stats.map(_.sizeInBytes.toLong).getOrElse(defaultTableSize))
    } else {
      new InMemoryFileIndex(
        ss, globbedPaths, options, Some(partitionSchema), fileStatusCache)
    }
    val relation = HadoopFsRelation(
      fileCatalog,
      partitionSchema = partitionSchema,
      dataSchema = dataSchema.asNullable,
      bucketSpec = bucketSpec,
      format,
      caseInsensitiveOptions)(ss)
  }

  test("file") {

    SparkSession.setActiveSession(ss)
    val hadoopConf = ss.sessionState.newHadoopConf()

    val format = DataSource.lookupDataSource("parquet").newInstance().asInstanceOf[FileFormat]

    val dataSchema = StructType(Seq(StructField("value", StringType)))
    val partitionSchema = StructType(Seq(StructField("key", IntegerType)))
    val schema = StructType(Seq(StructField("key", IntegerType), StructField("value", StringType)))

    val filter = In("key", Array("1", "2"))
    val readFunc = format.buildReaderWithPartitionValues(ss, dataSchema, partitionSchema, schema, Seq(filter), Map.empty[String, String], hadoopConf)

    val fs = FileSystem.get(hadoopConf)
    val selectedPartitions = fs.listStatus(new Path(path)).map { directory =>
      val v = InternalRow.fromSeq(Seq(directory.getPath.getName.split("=").last.toInt))
      val f = fs.listLocatedStatus(directory.getPath)
      val arr = ArrayBuffer.empty[LocatedFileStatus]
      while (f.hasNext) arr += f.next()
      PartitionDirectory(v, arr)
    }

    val options = Map.empty[String, String]
    format.inferSchema(ss, options,selectedPartitions.flatMap(_.files))
    val rdd = createRDD(readFunc, selectedPartitions, format,options)

    val needsUnsafeRowConversion: Boolean = if (format.isInstanceOf[ParquetFileFormat]) {
      SparkSession.getActiveSession.get.sessionState.conf.parquetVectorizedReaderEnabled
    } else {
      false
    }
   val scan =  if (needsUnsafeRowConversion) {
     rdd.mapPartitionsWithIndexInternal { (index, iter) =>
       val proj = UnsafeProjection.create(schema)
       proj.initialize(index)
       iter.map(proj)
     }
   }else rdd
    /*val (ctx, cleanedSource) = {
      val c = new CodegenContext
      (c,CodeFormatter.stripOverlappingComments(
        new CodeAndComment(CodeFormatter.stripExtraNewLines(""), c.getPlaceHolderToComments())))
    }
    val references=ctx.references.toArray
    rdd.mapPartitionsWithIndex{(index,iter)=>
      val clazz = CodeGenerator.compile(cleanedSource)
      val buffer = clazz.generate(references).asInstanceOf[BufferedRowIterator]
      buffer.init(index, Array(iter))
      new Iterator[InternalRow]{
        override def hasNext = buffer.hasNext

        override def next() = buffer.next()
      }
    }*/
    try {
      val arr = scan.collect()
      println(arr)
    } finally {
      StdIn.readLine()
    }
  }

  def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations
    case f => Array.empty[BlockLocation]
  }

  def getBlockHosts(
                     blockLocations: Array[BlockLocation], offset: Long, length: Long): Array[String] = {
    val candidates = blockLocations.map {
      // The fragment starts from a position within this block
      case b if b.getOffset <= offset && offset < b.getOffset + b.getLength =>
        b.getHosts -> (b.getOffset + b.getLength - offset).min(length)

      // The fragment ends at a position within this block
      case b if offset <= b.getOffset && offset + length < b.getLength =>
        b.getHosts -> (offset + length - b.getOffset).min(length)

      // The fragment fully contains this block
      case b if offset <= b.getOffset && b.getOffset + b.getLength <= offset + length =>
        b.getHosts -> b.getLength

      // The fragment doesn't intersect with this block
      case b =>
        b.getHosts -> 0L
    }.filter { case (hosts, size) =>
      size > 0L
    }

    if (candidates.isEmpty) {
      Array.empty[String]
    } else {
      val (hosts, _) = candidates.maxBy { case (_, size) => size }
      hosts
    }
  }

  def createRDD(
                 readFile: (PartitionedFile) => Iterator[InternalRow],
                 selectedPartitions: Seq[PartitionDirectory],
                 format: FileFormat,
                 options: Map[String, String]) = {
    val defaultMaxSplitBytes = ss.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = ss.sessionState.conf.filesOpenCostInBytes
    val defaultParallelism = ss.sparkContext.defaultParallelism
    val totalBytes = selectedPartitions.flatMap(_.files.map(_.getLen + openCostInBytes)).sum
    val bytesPerCore = totalBytes / defaultParallelism
    val maxSplitBytes = Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))

    val splitFiles = selectedPartitions.flatMap { partition =>
      partition.files.flatMap { file =>
        val blockLocations = getBlockLocations(file)
        if (format.isSplitable(
          ss, options, file.getPath)) {
          (0L until file.getLen by maxSplitBytes).map { offset =>
            val remaining = file.getLen - offset
            val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining
            val hosts = getBlockHosts(blockLocations, offset, size)
            PartitionedFile(
              partition.values, file.getPath.toUri.toString, offset, size, hosts)
          }
        } else {
          val hosts = getBlockHosts(blockLocations, 0, file.getLen)
          Seq(PartitionedFile(
            partition.values, file.getPath.toUri.toString, 0, file.getLen, hosts))
        }
      }
    }.sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val partitions = new ArrayBuffer[FilePartition]
    val currentFiles = new ArrayBuffer[PartitionedFile]
    var currentSize = 0L

    def closePartition(): Unit = {
      if (currentFiles.nonEmpty) {
        val newPartition =
          FilePartition(
            partitions.size,
            currentFiles.toArray.toSeq) // Copy to a new Array.
        partitions += newPartition
      }
      currentFiles.clear()
      currentSize = 0
    }

    splitFiles.foreach { file =>
      if (currentSize + file.length > maxSplitBytes) {
        closePartition()
      }
      // Add the given file to the current partition.
      currentSize += file.length + openCostInBytes
      currentFiles += file
    }
    closePartition()
    new FileScanRDD(ss, readFile, partitions)
  }
}

class SimpleFileScanRDD(@transient private val sparkSession: SparkSession,
                        readFunction: (PartitionedFile) => Iterator[InternalRow],
                        @transient val filePartitions: Seq[FilePartition]) extends RDD[InternalRow](ss.sparkContext, Nil) {
  override def compute(split: Partition, context: TaskContext) = {
    val parts = split.asInstanceOf[FilePartition].files.iterator
    parts.flatMap(readFunction)
  }

  override protected def getPartitions = filePartitions.toArray
}

