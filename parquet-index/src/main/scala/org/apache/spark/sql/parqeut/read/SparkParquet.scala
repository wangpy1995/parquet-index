package org.apache.spark.sql.parqeut.read

import org.apache.hadoop.fs._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

class SparkParquet(ss: SparkSession) extends Logging{

  def createNonBucketFileScanRDD(
                                  simpleName: String,
                                  dataSchema: StructType,
                                  partitionSchema: StructType,
                                  schema: StructType,
                                  filters: Seq[Filter],
                                  path: String,
                                  options: Map[String, String] = Map.empty) = {
    SparkSession.setActiveSession(ss)
    val hadoopConf = ss.sessionState.newHadoopConf()

    val format = DataSource.lookupDataSource(simpleName: String).newInstance().asInstanceOf[FileFormat]

    val readFunc = format.buildReaderWithPartitionValues(ss, dataSchema, partitionSchema, schema, filters, options, hadoopConf)

    val fs = FileSystem.get(hadoopConf)
    val selectedPartitions = fs.listStatus(new Path(path)).map { directory =>
      val v = InternalRow.fromSeq(Seq(directory.getPath.getName.split("=").last.toInt))
      val f = fs.listLocatedStatus(directory.getPath)
      val arr = ArrayBuffer.empty[LocatedFileStatus]
      while (f.hasNext) arr += f.next()
      PartitionDirectory(v, arr)
    }

    format.inferSchema(ss, options, selectedPartitions.flatMap(_.files))
    val rdd = createRDD(readFunc, selectedPartitions, format, options)

   /* val needsUnsafeRowConversion: Boolean = if (format.isInstanceOf[ParquetFileFormat]) {
      SparkSession.getActiveSession.get.sessionState.conf.parquetVectorizedReaderEnabled
    } else {
      false
    }
    if (needsUnsafeRowConversion) {
      rdd.mapPartitionsWithIndexInternal { (index, iter) =>
        val proj = UnsafeProjection.create(schema)
        proj.initialize(index)
        iter.map(proj)
      }
    }
    else*/ rdd
  }

  private def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations
    case f => Array.empty[BlockLocation]
  }

  private def getBlockHosts(
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

  private def createRDD(
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

  /*def doCodeGen(child:SparkPlan): (CodegenContext, CodeAndComment) = {
    val ctx = new CodegenContext
    val code = child.asInstanceOf[CodegenSupport].produce(ctx, this)
    val source = s"""
      public Object generate(Object[] references) {
        return new GeneratedIterator(references);
      }

      ${ctx.registerComment(s"""Codegend pipeline for\n${child.treeString.trim}""")}
      final class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {

        private Object[] references;
        private scala.collection.Iterator[] inputs;
        ${ctx.declareMutableStates()}

        public GeneratedIterator(Object[] references) {
          this.references = references;
        }

        public void init(int index, scala.collection.Iterator[] inputs) {
          partitionIndex = index;
          this.inputs = inputs;
          ${ctx.initMutableStates()}
          ${ctx.initPartition()}
        }

        ${ctx.declareAddedFunctions()}

        protected void processNext() throws java.io.IOException {
          ${code.trim}
        }
      }
      """.trim

    // try to compile, helpful for debug
    val cleanedSource = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(CodeFormatter.stripExtraNewLines(source), ctx.getPlaceHolderToComments()))

    logDebug(s"\n${CodeFormatter.format(cleanedSource)}")
    (ctx, cleanedSource)
  }*/
}