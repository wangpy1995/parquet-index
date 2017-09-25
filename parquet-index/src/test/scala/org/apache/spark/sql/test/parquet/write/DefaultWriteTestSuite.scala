package org.apache.spark.sql.test.parquet.write

import org.apache.commons.logging.LogFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.catalyst.expressions.objects.CreateExternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.execution.{QueryExecution, WholeStageCodegenExec}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.vectorized.ColumnarBatch
import org.apache.spark.sql.parqeut.read.{GeneratedIterator, ParquetSourceStrategy, SparkParquet}
import org.apache.spark.sql.sources.{EqualTo, In}
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
    val logicalRelation = LogicalRelation(dataSource.resolveRelation(checkFilesExist = false), table)
    ss.sessionState.executePlan(logicalRelation).sparkPlan
    val plan0 = ParquetSourceStrategy(ss.sessionState.executePlan(logicalRelation).optimizedPlan)
    val plan1 = ParquetSourceStrategy(ss.sessionState.executePlan(logicalRelation).optimizedPlan)
 val x : LogicalPlan =>Seq[RDD[InternalRow]]= plan =>{
      val output = plan.output
      val deserializer = CreateExternalRow(plan.output, plan.schema)
      val pl = ParquetSourceStrategy(plan)
      pl.flatMap { p =>
        val rdd = WholeStageCodegenExec(p).execute()
          .mapPartitionsWithIndexInternal { (index, iter) =>
            val projection = GenerateSafeProjection.generate(deserializer :: Nil, output)
            projection.initialize(index)
            iter.map(projection)
          }
        Iterator(rdd)
      }
    }
    /*println(x(plan0).map(_.collect().mkString(",")))
    println(x(plan1).map(_.collect().mkString(",")))*/
  }

  test("file") {
    val dataSchema = StructType(Seq(StructField("value", StringType)))
    val partitionSchema = StructType(Seq(StructField("key", IntegerType)))
    val schema = StructType(Seq(StructField("value", StringType)))
    val filters = Array("a", "b").map(c=>EqualTo("value",c))

    val sp = new SparkParquet(ss)
    val scan = sp.createNonBucketFileScanRDD("parquet", dataSchema, partitionSchema, schema, filters, path)
      .mapPartitionsInternal { iter =>
        val columns = iter.asInstanceOf[Iterator[ColumnarBatch]]
        columns.flatMap { col =>
          new Iterator[InternalRow] {
            private val it = col.rowIterator()

            override def hasNext = it.hasNext

            override def next() = it.next().copy()
          }
        }
      }.map { r =>
      r
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

