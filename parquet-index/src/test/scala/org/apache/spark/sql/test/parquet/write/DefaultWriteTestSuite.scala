package org.apache.spark.sql.test.parquet.write

import org.apache.commons.logging.LogFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.catalyst.expressions.objects.CreateExternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.parqeut.read.{GeneratedIterator, ParquetSourceStrategy, SparkParquet}
import org.apache.spark.sql.sources.In
import org.apache.spark.sql.test.parquet._
import org.apache.spark.sql.test.parquet.ss.implicits._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{Partition, TaskContext}
import org.scalatest.FunSuite

import scala.io.StdIn

class DefaultWriteTestSuite extends FunSuite {
  private lazy val log = LogFactory.getLog(classOf[DefaultWriteTestSuite])

  test("default_write") {
    val df = ss.sparkContext.parallelize(testData).coalesce(1).toDF("key", "value")
    df.write.partitionBy("key").parquet(path)
  }

  test("file_scan") {

    val df = ss.read.parquet(path)
    df.createTempView("df")
    val table = ss.sessionState.catalog.getTempViewOrPermanentTableMetadata(TableIdentifier("df"))

    val pathOption = Map("path" -> path)
    val dataSource =
      DataSource(
        ss,
        // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
        // inferred at runtime. We should still support it.
        userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
        partitionColumns = table.partitionColumnNames,
        bucketSpec = table.bucketSpec,
        className = "parquet",
        options = table.storage.properties ++ pathOption,
        catalogTable = Some(table))
    val plan = LogicalRelation(dataSource.resolveRelation(checkFilesExist = false), table)
    val output = plan.output
    val deserializer = CreateExternalRow(plan.output, plan.schema)

    val pl = ParquetSourceStrategy(plan)
    val x = pl.flatMap { p =>
      val rdd = WholeStageCodegenExec(p).execute()
        .mapPartitionsWithIndexInternal { (index, iter) =>
        val projection = GenerateSafeProjection.generate(deserializer :: Nil, output)
        projection.initialize(index)
        iter.map(projection)
      }
      rdd.collect()
      rdd.collect()
    }
    println(x.mkString(","))
  }

  test("file") {
    val dataSchema = StructType(Seq(StructField("value", StringType)))
    val partitionSchema = StructType(Seq(StructField("key", IntegerType)))
    val schema = StructType(Seq(StructField("key", IntegerType), StructField("value", StringType)))
    val filters = Seq(In("key", Array(1, 2)))

    val sp = new SparkParquet(ss)
    val scan = sp.createNonBucketFileScanRDD("parquet", dataSchema, partitionSchema, schema, filters, path)
      .mapPartitionsWithIndexInternal { (index, iter) =>
        val buffer = new GeneratedIterator(Array(SQLMetrics.createMetric(ss.sparkContext, "number of output rows"), SQLMetrics.createTimingMetric(ss.sparkContext, "scan time total (min, med, max)")))
        buffer.init(index, Array(iter))
        new Iterator[InternalRow] {
          override def hasNext: Boolean = buffer.hasNext

          override def next: InternalRow = buffer.next()
        }
      }.mapPartitionsWithIndexInternal { (index, iter) =>
      val attributes = Seq[Attribute](AttributeReference("key", IntegerType)(), AttributeReference("value", StringType)())
      val deserializer = CreateExternalRow(attributes, schema)
      val projection = GenerateSafeProjection.generate(deserializer :: Nil, attributes)
      projection.initialize(index)
      iter.map(projection)
    }
    try {
      val arr = scan.collect()
      println(arr)
    } finally {
      StdIn.readLine()
    }
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

